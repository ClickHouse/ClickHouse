#include <Dictionaries/RangeHashedDictionary.h>
#include <Dictionaries/RangeDictionaryBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARY_IS_EMPTY;
    extern const int TYPE_MISMATCH;
}


RangeHashedDictionary::RangeHashedDictionary(
    const std::string & name, const DictionaryStructure & dict_struct, DictionarySourcePtr source_ptr,
    const DictionaryLifetime dict_lifetime, bool require_nonempty)
    : name{name}, dict_struct(dict_struct),
        source_ptr{std::move(source_ptr)}, dict_lifetime(dict_lifetime),
        require_nonempty(require_nonempty)
{
    createAttributes();

    try
    {
        loadData();
        calculateBytesAllocated();
    }
    catch (...)
    {
        creation_exception = std::current_exception();
    }

    creation_time = std::chrono::system_clock::now();
}

RangeHashedDictionary::RangeHashedDictionary(const RangeHashedDictionary & other)
    : RangeHashedDictionary{other.name, other.dict_struct, other.source_ptr->clone(), other.dict_lifetime, other.require_nonempty}
{
}


#define DECLARE_MULTIPLE_GETTER(TYPE)\
void RangeHashedDictionary::get##TYPE(\
    const std::string & attribute_name, const PaddedPODArray<Key> & ids, const PaddedPODArray<UInt16> & dates,\
    PaddedPODArray<TYPE> & out) const\
{\
    const auto & attribute = getAttributeWithType(attribute_name, AttributeUnderlyingType::TYPE);\
    getItems<TYPE>(attribute, ids, dates, out);\
}
DECLARE_MULTIPLE_GETTER(UInt8)
DECLARE_MULTIPLE_GETTER(UInt16)
DECLARE_MULTIPLE_GETTER(UInt32)
DECLARE_MULTIPLE_GETTER(UInt64)
DECLARE_MULTIPLE_GETTER(Int8)
DECLARE_MULTIPLE_GETTER(Int16)
DECLARE_MULTIPLE_GETTER(Int32)
DECLARE_MULTIPLE_GETTER(Int64)
DECLARE_MULTIPLE_GETTER(Float32)
DECLARE_MULTIPLE_GETTER(Float64)
#undef DECLARE_MULTIPLE_GETTER

void RangeHashedDictionary::getString(
    const std::string & attribute_name, const PaddedPODArray<Key> & ids, const PaddedPODArray<UInt16> & dates,
    ColumnString * out) const
{
    const auto & attribute = getAttributeWithType(attribute_name, AttributeUnderlyingType::String);
    const auto & attr = *std::get<Ptr<StringRef>>(attribute.maps);
    const auto & null_value = std::get<String>(attribute.null_values);

    for (const auto i : ext::range(0, ids.size()))
    {
        const auto it = attr.find(ids[i]);
        if (it != std::end(attr))
        {
            const auto date = dates[i];
            const auto & ranges_and_values = it->second;
            const auto val_it = std::find_if(std::begin(ranges_and_values), std::end(ranges_and_values),
                [date] (const Value<StringRef> & v) { return v.range.contains(date); });

            const auto string_ref = val_it != std::end(ranges_and_values) ? val_it->value : StringRef{null_value};
            out->insertData(string_ref.data, string_ref.size);
        }
        else
            out->insertData(null_value.data(), null_value.size());
    }

    query_count.fetch_add(ids.size(), std::memory_order_relaxed);
}


void RangeHashedDictionary::createAttributes()
{
    const auto size = dict_struct.attributes.size();
    attributes.reserve(size);

    for (const auto & attribute : dict_struct.attributes)
    {
        attribute_index_by_name.emplace(attribute.name, attributes.size());
        attributes.push_back(createAttributeWithType(attribute.underlying_type, attribute.null_value));

        if (attribute.hierarchical)
            throw Exception{
                name + ": hierarchical attributes not supported by " + getName() + " dictionary.",
                ErrorCodes::BAD_ARGUMENTS};
    }
}

void RangeHashedDictionary::loadData()
{
    auto stream = source_ptr->loadAll();
    stream->readPrefix();

    while (const auto block = stream->read())
    {
        const auto & id_column = *block.safeGetByPosition(0).column;
        const auto & min_range_column = *block.safeGetByPosition(1).column;
        const auto & max_range_column = *block.safeGetByPosition(2).column;

        element_count += id_column.size();

        for (const auto attribute_idx : ext::range(0, attributes.size()))
        {
            const auto & attribute_column = *block.safeGetByPosition(attribute_idx + 3).column;
            auto & attribute = attributes[attribute_idx];

            for (const auto row_idx : ext::range(0, id_column.size()))
                setAttributeValue(attribute, id_column[row_idx].get<UInt64>(),
                    Range(min_range_column[row_idx].get<UInt64>(), max_range_column[row_idx].get<UInt64>()),
                    attribute_column[row_idx]);
        }
    }

    stream->readSuffix();

    if (require_nonempty && 0 == element_count)
        throw Exception{
            name + ": dictionary source is empty and 'require_nonempty' property is set.",
            ErrorCodes::DICTIONARY_IS_EMPTY};
}

template <typename T>
void RangeHashedDictionary::addAttributeSize(const Attribute & attribute)
{
    const auto & map_ref = std::get<Ptr<T>>(attribute.maps);
    bytes_allocated += sizeof(Collection<T>) + map_ref->getBufferSizeInBytes();
    bucket_count = map_ref->getBufferSizeInCells();
}

void RangeHashedDictionary::calculateBytesAllocated()
{
    bytes_allocated += attributes.size() * sizeof(attributes.front());

    for (const auto & attribute : attributes)
    {
        switch (attribute.type)
        {
            case AttributeUnderlyingType::UInt8: addAttributeSize<UInt8>(attribute); break;
            case AttributeUnderlyingType::UInt16: addAttributeSize<UInt16>(attribute); break;
            case AttributeUnderlyingType::UInt32: addAttributeSize<UInt32>(attribute); break;
            case AttributeUnderlyingType::UInt64: addAttributeSize<UInt64>(attribute); break;
            case AttributeUnderlyingType::Int8: addAttributeSize<Int8>(attribute); break;
            case AttributeUnderlyingType::Int16: addAttributeSize<Int16>(attribute); break;
            case AttributeUnderlyingType::Int32: addAttributeSize<Int32>(attribute); break;
            case AttributeUnderlyingType::Int64: addAttributeSize<Int64>(attribute); break;
            case AttributeUnderlyingType::Float32: addAttributeSize<Float32>(attribute); break;
            case AttributeUnderlyingType::Float64: addAttributeSize<Float64>(attribute); break;
            case AttributeUnderlyingType::String:
            {
                addAttributeSize<StringRef>(attribute);
                bytes_allocated += sizeof(Arena) + attribute.string_arena->size();

                break;
            }
        }
    }
}

template <typename T>
void RangeHashedDictionary::createAttributeImpl(Attribute & attribute, const Field & null_value)
{
    std::get<T>(attribute.null_values) = null_value.get<typename NearestFieldType<T>::Type>();
    std::get<Ptr<T>>(attribute.maps) = std::make_unique<Collection<T>>();
}

RangeHashedDictionary::Attribute RangeHashedDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
{
    Attribute attr{type};

    switch (type)
    {
        case AttributeUnderlyingType::UInt8: createAttributeImpl<UInt8>(attr, null_value); break;
        case AttributeUnderlyingType::UInt16: createAttributeImpl<UInt16>(attr, null_value); break;
        case AttributeUnderlyingType::UInt32: createAttributeImpl<UInt32>(attr, null_value); break;
        case AttributeUnderlyingType::UInt64: createAttributeImpl<UInt64>(attr, null_value); break;
        case AttributeUnderlyingType::Int8: createAttributeImpl<Int8>(attr, null_value); break;
        case AttributeUnderlyingType::Int16: createAttributeImpl<Int16>(attr, null_value); break;
        case AttributeUnderlyingType::Int32: createAttributeImpl<Int32>(attr, null_value); break;
        case AttributeUnderlyingType::Int64: createAttributeImpl<Int64>(attr, null_value); break;
        case AttributeUnderlyingType::Float32: createAttributeImpl<Float32>(attr, null_value); break;
        case AttributeUnderlyingType::Float64: createAttributeImpl<Float64>(attr, null_value); break;
        case AttributeUnderlyingType::String:
        {
            std::get<String>(attr.null_values) = null_value.get<String>();
            std::get<Ptr<StringRef>>(attr.maps) = std::make_unique<Collection<StringRef>>();
            attr.string_arena = std::make_unique<Arena>();
            break;
        }
    }

    return attr;
}


template <typename OutputType>
void RangeHashedDictionary::getItems(
    const Attribute & attribute,
    const PaddedPODArray<Key> & ids,
    const PaddedPODArray<UInt16> & dates,
    PaddedPODArray<OutputType> & out) const
{
    if (false) {}
#define DISPATCH(TYPE) \
    else if (attribute.type == AttributeUnderlyingType::TYPE) \
        getItemsImpl<TYPE, OutputType>(attribute, ids, dates, out);
    DISPATCH(UInt8)
    DISPATCH(UInt16)
    DISPATCH(UInt32)
    DISPATCH(UInt64)
    DISPATCH(Int8)
    DISPATCH(Int16)
    DISPATCH(Int32)
    DISPATCH(Int64)
    DISPATCH(Float32)
    DISPATCH(Float64)
#undef DISPATCH
    else
        throw Exception("Unexpected type of attribute: " + toString(attribute.type), ErrorCodes::LOGICAL_ERROR);
}

template <typename AttributeType, typename OutputType>
void RangeHashedDictionary::getItemsImpl(
    const Attribute & attribute,
    const PaddedPODArray<Key> & ids,
    const PaddedPODArray<UInt16> & dates,
    PaddedPODArray<OutputType> & out) const
{
    const auto & attr = *std::get<Ptr<AttributeType>>(attribute.maps);
    const auto null_value = std::get<AttributeType>(attribute.null_values);

    for (const auto i : ext::range(0, ids.size()))
    {
        const auto it = attr.find(ids[i]);
        if (it != std::end(attr))
        {
            const auto date = dates[i];
            const auto & ranges_and_values = it->second;
            const auto val_it = std::find_if(std::begin(ranges_and_values), std::end(ranges_and_values),
                [date] (const Value<AttributeType> & v) { return v.range.contains(date); });

            out[i] = val_it != std::end(ranges_and_values) ? val_it->value : null_value;
        }
        else
            out[i] = null_value;
    }

    query_count.fetch_add(ids.size(), std::memory_order_relaxed);
}


template <typename T>
void RangeHashedDictionary::setAttributeValueImpl(Attribute & attribute, const Key id, const Range & range, const T value)
{
    auto & map = *std::get<Ptr<T>>(attribute.maps);
    const auto it = map.find(id);

    if (it != map.end())
    {
        auto & values = it->second;

        const auto insert_it = std::lower_bound(std::begin(values), std::end(values), range,
            [] (const Value<T> & lhs, const Range & range) {
                return lhs.range < range;
            });

        values.insert(insert_it, Value<T>{ range, value });
    }
    else
        map.insert({ id, Values<T>{ Value<T>{ range, value } } });
}

void RangeHashedDictionary::setAttributeValue(Attribute & attribute, const Key id, const Range & range, const Field & value)
{
    switch (attribute.type)
    {
        case AttributeUnderlyingType::UInt8: setAttributeValueImpl<UInt8>(attribute, id, range, value.get<UInt64>()); break;
        case AttributeUnderlyingType::UInt16: setAttributeValueImpl<UInt16>(attribute, id, range, value.get<UInt64>()); break;
        case AttributeUnderlyingType::UInt32: setAttributeValueImpl<UInt32>(attribute, id, range, value.get<UInt64>()); break;
        case AttributeUnderlyingType::UInt64: setAttributeValueImpl<UInt64>(attribute, id, range, value.get<UInt64>()); break;
        case AttributeUnderlyingType::Int8: setAttributeValueImpl<Int8>(attribute, id, range, value.get<Int64>()); break;
        case AttributeUnderlyingType::Int16: setAttributeValueImpl<Int16>(attribute, id, range, value.get<Int64>()); break;
        case AttributeUnderlyingType::Int32: setAttributeValueImpl<Int32>(attribute, id, range, value.get<Int64>()); break;
        case AttributeUnderlyingType::Int64: setAttributeValueImpl<Int64>(attribute, id, range, value.get<Int64>()); break;
        case AttributeUnderlyingType::Float32: setAttributeValueImpl<Float32>(attribute, id, range, value.get<Float64>()); break;
        case AttributeUnderlyingType::Float64: setAttributeValueImpl<Float64>(attribute, id, range, value.get<Float64>()); break;
        case AttributeUnderlyingType::String:
        {
            auto & map = *std::get<Ptr<StringRef>>(attribute.maps);
            const auto & string = value.get<String>();
            const auto string_in_arena = attribute.string_arena->insert(string.data(), string.size());
            const StringRef string_ref{string_in_arena, string.size()};

            const auto it = map.find(id);

            if (it != map.end())
            {
                auto & values = it->second;

                const auto insert_it = std::lower_bound(std::begin(values), std::end(values), range,
                    [] (const Value<StringRef> & lhs, const Range & range) {
                        return lhs.range < range;
                    });

                values.insert(insert_it, Value<StringRef>{ range, string_ref });
            }
            else
                map.insert({ id, Values<StringRef>{ Value<StringRef>{ range, string_ref } } });

            break;
        }
    }
}

const RangeHashedDictionary::Attribute & RangeHashedDictionary::getAttribute(const std::string & attribute_name) const
{
    const auto it = attribute_index_by_name.find(attribute_name);
    if (it == std::end(attribute_index_by_name))
        throw Exception{
            name + ": no such attribute '" + attribute_name + "'",
            ErrorCodes::BAD_ARGUMENTS};

    return attributes[it->second];
}

const RangeHashedDictionary::Attribute & RangeHashedDictionary::getAttributeWithType(const std::string & name, const AttributeUnderlyingType type) const
{
    const auto & attribute = getAttribute(name);
    if (attribute.type != type)
        throw Exception{
            name + ": type mismatch: attribute " + name + " has type " + toString(attribute.type),
            ErrorCodes::TYPE_MISMATCH};

    return attribute;
}

void RangeHashedDictionary::getIdsAndDates(PaddedPODArray<Key> & ids,
                                           PaddedPODArray<UInt16> & start_dates, PaddedPODArray<UInt16> & end_dates) const
{
    const auto & attribute = attributes.front();

    switch (attribute.type)
    {
        case AttributeUnderlyingType::UInt8: getIdsAndDates<UInt8>(attribute, ids, start_dates, end_dates); break;
        case AttributeUnderlyingType::UInt16: getIdsAndDates<UInt16>(attribute, ids, start_dates, end_dates); break;
        case AttributeUnderlyingType::UInt32: getIdsAndDates<UInt32>(attribute, ids, start_dates, end_dates); break;
        case AttributeUnderlyingType::UInt64: getIdsAndDates<UInt64>(attribute, ids, start_dates, end_dates); break;
        case AttributeUnderlyingType::Int8: getIdsAndDates<Int8>(attribute, ids, start_dates, end_dates); break;
        case AttributeUnderlyingType::Int16: getIdsAndDates<Int16>(attribute, ids, start_dates, end_dates); break;
        case AttributeUnderlyingType::Int32: getIdsAndDates<Int32>(attribute, ids, start_dates, end_dates); break;
        case AttributeUnderlyingType::Int64: getIdsAndDates<Int64>(attribute, ids, start_dates, end_dates); break;
        case AttributeUnderlyingType::Float32: getIdsAndDates<Float32>(attribute, ids, start_dates, end_dates); break;
        case AttributeUnderlyingType::Float64: getIdsAndDates<Float64>(attribute, ids, start_dates, end_dates); break;
        case AttributeUnderlyingType::String: getIdsAndDates<StringRef>(attribute, ids, start_dates, end_dates); break;
    }
}

template <typename T>
void RangeHashedDictionary::getIdsAndDates(const Attribute& attribute, PaddedPODArray<Key> & ids,
                                           PaddedPODArray<UInt16> & start_dates, PaddedPODArray<UInt16> & end_dates) const
{
    const HashMap<UInt64, Values<T>> & attr = *std::get<Ptr<T>>(attribute.maps);

    ids.reserve(attr.size());
    start_dates.reserve(attr.size());
    end_dates.reserve(attr.size());

    for (const auto & key : attr) {
        ids.push_back(key.first);
        for (const auto & value : key.second)
        {
            start_dates.push_back(value.range.first);
            end_dates.push_back(value.range.second);
        }
    }
}

BlockInputStreamPtr RangeHashedDictionary::getBlockInputStream(const Names & column_names, size_t max_block_size) const
{
    PaddedPODArray<Key> ids;
    PaddedPODArray<UInt16> start_dates;
    PaddedPODArray<UInt16> end_dates;
    getIdsAndDates(ids, start_dates, end_dates);

    using BlockInputStreamType = RangeDictionaryBlockInputStream<RangeHashedDictionary, Key>;
    auto dict_ptr = std::static_pointer_cast<const RangeHashedDictionary>(shared_from_this());
    return std::make_shared<BlockInputStreamType>(
        dict_ptr, max_block_size, column_names, std::move(ids), std::move(start_dates), std::move(end_dates));
}


}
