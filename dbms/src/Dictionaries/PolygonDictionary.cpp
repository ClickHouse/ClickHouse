#include <ext/map.h>
#include "PolygonDictionary.h"
#include "DictionaryBlockInputStream.h"
#include "DictionaryFactory.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TYPE;
    extern const int UNSUPPORTED_METHOD;
}


IPolygonDictionary::IPolygonDictionary(
        const std::string & name_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        const DictionaryLifetime dict_lifetime_)
        : name(name_)
        , dict_struct(dict_struct_)
        , source_ptr(std::move(source_ptr_))
        , dict_lifetime(dict_lifetime_)
{
}

std::string IPolygonDictionary::getName() const
{
    return name;
}

std::string IPolygonDictionary::getTypeName() const
{
    return "Polygon";
}

size_t IPolygonDictionary::getBytesAllocated() const
{
    return bytes_allocated;
}

size_t IPolygonDictionary::getQueryCount() const
{
    return query_count.load(std::memory_order_relaxed);
}

double IPolygonDictionary::getHitRate() const
{
    return 1.0;
}

size_t IPolygonDictionary::getElementCount() const
{
    return element_count;
}

double IPolygonDictionary::getLoadFactor() const
{
    return 1.0;
}

const IDictionarySource * IPolygonDictionary::getSource() const
{
    return source_ptr.get();
}

const DictionaryLifetime & IPolygonDictionary::getLifetime() const
{
    return dict_lifetime;
}

const DictionaryStructure & IPolygonDictionary::getStructure() const
{
    return dict_struct;
}

bool IPolygonDictionary::isInjective(const std::string &) const
{
    return false;
}

BlockInputStreamPtr IPolygonDictionary::getBlockInputStream(const Names &, size_t) const {
    // TODO: Better error message.
    throw Exception{"Reading the dictionary is not allowed", ErrorCodes::UNSUPPORTED_METHOD};
}

void IPolygonDictionary::createAttributes() {
    for (size_t i = 0; i < dict_struct.attributes.size(); ++i)
    {
        attribute_index_by_name.emplace(dict_struct.attributes[i].name, i);

        if (dict_struct.attributes[i].hierarchical)
            throw Exception{name + ": hierarchical attributes not supported for dictionary of type " + getTypeName(),
                            ErrorCodes::TYPE_MISMATCH};
    }
}

void IPolygonDictionary::blockToAttributes(const DB::Block &block) {
    const auto rows = block.rows();
    element_count += rows;
    // TODO: Only save columns for attributes, since we are converting the key to a boost type separately.
    blocks.push_back(block);
    polygons.reserve(polygons.size() + rows);

    const auto & key = block.safeGetByPosition(0).column;

    for (const auto row : ext::range(0, rows))
    {
        const auto & field = (*key)[row];
        polygons.push_back(fieldToMultiPolygon(field));
    }
}

void IPolygonDictionary::loadData() {
    auto stream = source_ptr->loadAll();
    stream->readPrefix();
    while (const auto block = stream->read()) {
        blockToAttributes(block);
    }
    stream->readSuffix();
}

void IPolygonDictionary::calculateBytesAllocated()
{
    for (const auto & block : blocks)
        bytes_allocated += block.allocatedBytes();

}

void IPolygonDictionary::has(const Columns &key_columns, const DataTypes &key_types, PaddedPODArray<UInt8> &out) {
    // TODO: Use constant in error message?
    if (key_types.size() != DIM)
        throw Exception{"Expected two columns of coordinates", ErrorCodes::BAD_ARGUMENTS};
    for (const auto i : ext::range(0, DIM))
    {
        // TODO: Not sure if this is the best way to check.
        if (key_types[i]->getName() != "Array(Float64)")
            throw Exception{"Expected an array of Float64", ErrorCodes::TYPE_MISMATCH};
    }
    const auto rows = key_columns.front()->size();
    for (const auto row : ext::range(0, rows))
    {
        Point pt(key_columns[0]->getFloat64(row), key_columns[1]->getFloat64(row));
        // TODO: Check whether this will be optimized by the compiler.
        size_t trash;
        out[row] = find(pt, trash);
    }
}

IPolygonDictionary::Point IPolygonDictionary::fieldToPoint(const Field &field)
{
    if (field.getType() == Field::Types::Array)
    {
        auto coordinate_array = field.get<Array>();
        if (coordinate_array.size() != DIM)
            throw Exception{"All points should be two-dimensional", ErrorCodes::LOGICAL_ERROR};
        Float64 values[DIM];
        for (size_t i = 0; i < DIM; ++i)
        {
            if (coordinate_array[i].getType() != Field::Types::Float64)
                throw Exception{"Coordinates should be Float64", ErrorCodes::TYPE_MISMATCH};
            values[i] = coordinate_array[i].get<Float64>();
        }
        return {values[0], values[1]};
    }
    else
        throw Exception{"Point is not represented by an array", ErrorCodes::TYPE_MISMATCH};
}

IPolygonDictionary::Polygon IPolygonDictionary::fieldToPolygon(const Field & field) {
    Polygon result;
    if (field.getType() == Field::Types::Array)
    {
        const auto & ring_array = field.get<Array>();
        if (ring_array.empty())
            throw Exception{"Empty polygons are not allowed", ErrorCodes::LOGICAL_ERROR};
        result.inners().resize(ring_array.size() - 1);
        if (ring_array[0].getType() != Field::Types::Array)
            throw Exception{"Outer polygon ring is not represented by an array", ErrorCodes::TYPE_MISMATCH};
        for (const auto & point : ring_array[0].get<Array>())
            bg::append(result.outer(), fieldToPoint(point));
        for (size_t i = 0; i < result.inners().size(); ++i) {
            if (ring_array[i + 1].getType() != Field::Types::Array)
                throw Exception{"Inner polygon ring is not represented by an array", ErrorCodes::TYPE_MISMATCH};
            for (const auto & point : ring_array[i + 1].get<Array>())
                bg::append(result.inners()[i], fieldToPoint(point));
        }
    }
    else
        throw Exception{"Polygon is not represented by an array", ErrorCodes::TYPE_MISMATCH};
    return result;
}

IPolygonDictionary::MultiPolygon IPolygonDictionary::fieldToMultiPolygon(const Field &field) {
    MultiPolygon result;
    if (field.getType() == Field::Types::Array)
    {
        const auto& polygon_array = field.get<Array>();
        result.reserve(polygon_array.size());
        for (const auto & polygon : polygon_array)
            result.push_back(fieldToPolygon(polygon));
    }
    else
        throw Exception{"MultiPolygon is not represented by an array", ErrorCodes::TYPE_MISMATCH};
    return result;
}

SimplePolygonDictionary::SimplePolygonDictionary(
        const std::string & name_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        const DictionaryLifetime dict_lifetime_)
        : IPolygonDictionary(name_, dict_struct_, std::move(source_ptr_), dict_lifetime_)
{
}

std::shared_ptr<const IExternalLoadable> SimplePolygonDictionary::clone() const
{
    return std::make_shared<SimplePolygonDictionary>(
            this->name,
            this->dict_struct,
            this->source_ptr->clone(),
            this->dict_lifetime);
}

void SimplePolygonDictionary::generate() {}

bool SimplePolygonDictionary::find(const Point &point, size_t & id) const
{
    for (size_t i = 0; i < (this->polygons).size(); ++i)
    {
        if (bg::within(point, (this->polygons)[i])) {
            id = i;
            return true;
        }
    }
    return false;
}

void registerDictionaryPolygon(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string & name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr) -> DictionaryPtr
    {
        // TODO: Check that there is only one key and it is of the correct type.
        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception{name
                            + ": elements .structure.range_min and .structure.range_max should be defined only "
                              "for a dictionary of layout 'range_hashed'",
                            ErrorCodes::BAD_ARGUMENTS};
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        return std::make_unique<SimplePolygonDictionary>(name, dict_struct, std::move(source_ptr), dict_lifetime);
    };
    factory.registerLayout("polygon", create_layout, true);
}

}