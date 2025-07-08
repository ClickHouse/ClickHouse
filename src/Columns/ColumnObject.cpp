#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnCompressed.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Common/Arena.h>
#include <Common/SipHash.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

namespace
{

const FormatSettings & getFormatSettings()
{
    static const FormatSettings settings;
    return settings;
}

const std::shared_ptr<SerializationDynamic> & getDynamicSerialization()
{
    static const std::shared_ptr<SerializationDynamic> dynamic_serialization = std::make_shared<SerializationDynamic>();
    return dynamic_serialization;
}

struct ColumnObjectCheckpoint : public ColumnCheckpoint
{
    using CheckpointsMap = std::unordered_map<std::string_view, ColumnCheckpointPtr>;

    ColumnObjectCheckpoint(size_t size_, CheckpointsMap typed_paths_, CheckpointsMap dynamic_paths_, ColumnCheckpointPtr shared_data_)
        : ColumnCheckpoint(size_)
        , typed_paths(std::move(typed_paths_))
        , dynamic_paths(std::move(dynamic_paths_))
        , shared_data(std::move(shared_data_))
    {
    }

    CheckpointsMap typed_paths;
    CheckpointsMap dynamic_paths;
    ColumnCheckpointPtr shared_data;
};

}

ColumnObject::ColumnObject(
    std::unordered_map<String, MutableColumnPtr> typed_paths_,
    std::unordered_map<String, MutableColumnPtr> dynamic_paths_,
    MutableColumnPtr shared_data_,
    size_t max_dynamic_paths_,
    size_t global_max_dynamic_paths_,
    size_t max_dynamic_types_,
    const StatisticsPtr & statistics_)
    : shared_data(std::move(shared_data_))
    , max_dynamic_paths(max_dynamic_paths_)
    , global_max_dynamic_paths(global_max_dynamic_paths_)
    , max_dynamic_types(max_dynamic_types_)
    , statistics(statistics_)
{
    typed_paths.reserve(typed_paths_.size());
    sorted_typed_paths.reserve(typed_paths_.size());
    for (auto & [path, column] : typed_paths_)
    {
        auto it = typed_paths.emplace(path, std::move(column)).first;
        sorted_typed_paths.push_back(it->first);
    }
    std::sort(sorted_typed_paths.begin(), sorted_typed_paths.end());

    dynamic_paths.reserve(dynamic_paths_.size());
    dynamic_paths_ptrs.reserve(dynamic_paths_.size());
    for (auto & [path, column] : dynamic_paths_)
    {
        auto it = dynamic_paths.emplace(path, std::move(column)).first;
        dynamic_paths_ptrs[path] = assert_cast<ColumnDynamic *>(it->second.get());
        sorted_dynamic_paths.insert(it->first);
    }
}

ColumnObject::ColumnObject(
    std::unordered_map<String, MutableColumnPtr> typed_paths_, size_t max_dynamic_paths_, size_t max_dynamic_types_)
    : max_dynamic_paths(max_dynamic_paths_), global_max_dynamic_paths(max_dynamic_paths_), max_dynamic_types(max_dynamic_types_)
{
    typed_paths.reserve(typed_paths_.size());
    sorted_typed_paths.reserve(typed_paths_.size());
    for (auto & [path, column] : typed_paths_)
    {
        if (!column->empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected non-empty typed path column in ColumnObject constructor");
        auto it = typed_paths.emplace(path, std::move(column)).first;
        sorted_typed_paths.push_back(it->first);
    }

    std::sort(sorted_typed_paths.begin(), sorted_typed_paths.end());

    MutableColumns paths_and_values;
    paths_and_values.emplace_back(ColumnString::create());
    paths_and_values.emplace_back(ColumnString::create());
    shared_data = ColumnArray::create(ColumnTuple::create(std::move(paths_and_values)));
}

ColumnObject::ColumnObject(const ColumnObject & other)
    : COWHelper<IColumnHelper<ColumnObject>, ColumnObject>(other)
    , typed_paths(other.typed_paths)
    , dynamic_paths(other.dynamic_paths)
    , dynamic_paths_ptrs(other.dynamic_paths_ptrs)
    , shared_data(other.shared_data)
    , max_dynamic_paths(other.max_dynamic_paths)
    , global_max_dynamic_paths(other.global_max_dynamic_paths)
    , max_dynamic_types(other.max_dynamic_types)
    , statistics(other.statistics)
{
    /// We should update string_view in sorted_typed_paths and sorted_dynamic_paths so they
    /// point to the new strings in typed_paths and dynamic_paths.
    sorted_typed_paths.clear();
    for (const auto & [path, _] : typed_paths)
        sorted_typed_paths.emplace_back(path);
    std::sort(sorted_typed_paths.begin(), sorted_typed_paths.end());

    sorted_dynamic_paths.clear();
    for (const auto & [path, _] : dynamic_paths)
        sorted_dynamic_paths.emplace(path);
}

ColumnObject::Ptr ColumnObject::create(
    const std::unordered_map<String, ColumnPtr> & typed_paths_,
    const std::unordered_map<String, ColumnPtr> & dynamic_paths_,
    const ColumnPtr & shared_data_,
    size_t max_dynamic_paths_,
    size_t global_max_dynamic_paths_,
    size_t max_dynamic_types_,
    const ColumnObject::StatisticsPtr & statistics_)
{
    std::unordered_map<String, MutableColumnPtr> mutable_typed_paths;
    mutable_typed_paths.reserve(typed_paths_.size());
    for (const auto & [path, column] : typed_paths_)
        mutable_typed_paths[path] = typed_paths_.at(path)->assumeMutable();

    std::unordered_map<String, MutableColumnPtr> mutable_dynamic_paths;
    mutable_dynamic_paths.reserve(dynamic_paths_.size());
    for (const auto & [path, column] : dynamic_paths_)
        mutable_dynamic_paths[path] = dynamic_paths_.at(path)->assumeMutable();

    return ColumnObject::create(
        std::move(mutable_typed_paths),
        std::move(mutable_dynamic_paths),
        shared_data_->assumeMutable(),
        max_dynamic_paths_,
        global_max_dynamic_paths_,
        max_dynamic_types_,
        statistics_);
}

ColumnObject::MutablePtr ColumnObject::create(
    std::unordered_map<String, MutableColumnPtr> typed_paths_,
    std::unordered_map<String, MutableColumnPtr> dynamic_paths_,
    MutableColumnPtr shared_data_,
    size_t max_dynamic_paths_,
    size_t global_max_dynamic_paths_,
    size_t max_dynamic_types_,
    const ColumnObject::StatisticsPtr & statistics_)
{
    return Base::create(std::move(typed_paths_), std::move(dynamic_paths_), std::move(shared_data_), max_dynamic_paths_, global_max_dynamic_paths_, max_dynamic_types_, statistics_);
}

ColumnObject::MutablePtr ColumnObject::create(std::unordered_map<String, MutableColumnPtr> typed_paths_, size_t max_dynamic_paths_, size_t max_dynamic_types_)
{
    return Base::create(std::move(typed_paths_), max_dynamic_paths_, max_dynamic_types_);
}

std::string ColumnObject::getName() const
{
    WriteBufferFromOwnString ss;
    ss << "Object(";
    ss << "max_dynamic_paths=" << global_max_dynamic_paths;
    ss << ", max_dynamic_types=" << max_dynamic_types;
    for (const auto & path : sorted_typed_paths)
        ss << ", " << path << " " << typed_paths.find(path)->second->getName();
    ss << ")";
    return ss.str();
}

MutableColumnPtr ColumnObject::cloneEmpty() const
{
    std::unordered_map<String, MutableColumnPtr> empty_typed_paths;
    empty_typed_paths.reserve(typed_paths.size());
    for (const auto & [path, column] : typed_paths)
        empty_typed_paths[path] = column->cloneEmpty();

    std::unordered_map<String, MutableColumnPtr> empty_dynamic_paths;
    empty_dynamic_paths.reserve(dynamic_paths.size());
    for (const auto & [path, column] : dynamic_paths)
        empty_dynamic_paths[path] = column->cloneEmpty();

    return ColumnObject::create(
        std::move(empty_typed_paths),
        std::move(empty_dynamic_paths),
        shared_data->cloneEmpty(),
        max_dynamic_paths,
        global_max_dynamic_paths,
        max_dynamic_types,
        statistics);
}

MutableColumnPtr ColumnObject::cloneResized(size_t size) const
{
    std::unordered_map<String, MutableColumnPtr> resized_typed_paths;
    resized_typed_paths.reserve(typed_paths.size());
    for (const auto & [path, column] : typed_paths)
        resized_typed_paths[path] = column->cloneResized(size);

    std::unordered_map<String, MutableColumnPtr> resized_dynamic_paths;
    resized_dynamic_paths.reserve(dynamic_paths.size());
    for (const auto & [path, column] : dynamic_paths)
        resized_dynamic_paths[path] = column->cloneResized(size);

    return ColumnObject::create(
        std::move(resized_typed_paths),
        std::move(resized_dynamic_paths),
        shared_data->cloneResized(size),
        max_dynamic_paths,
        global_max_dynamic_paths,
        max_dynamic_types,
        statistics);
}

Field ColumnObject::operator[](size_t n) const
{
    Object object;

    for (const auto & [path, column] : typed_paths)
        object[path] = (*column)[n];
    for (const auto & [path, column] : dynamic_paths_ptrs)
    {
        /// Output only non-null values from dynamic paths. We cannot distinguish cases when
        /// dynamic path has Null value and when it's absent in the row and consider them equivalent.
        if (!column->isNullAt(n))
            object[path] = (*column)[n];
    }

    const auto & shared_data_offsets = getSharedDataOffsets();
    const auto [shared_paths, shared_values] = getSharedDataPathsAndValues();
    size_t start = shared_data_offsets[static_cast<ssize_t>(n) - 1];
    size_t end = shared_data_offsets[n];
    for (size_t i = start; i != end; ++i)
    {
        String path = shared_paths->getDataAt(i).toString();
        auto value_data = shared_values->getDataAt(i);
        ReadBufferFromMemory buf(value_data.data, value_data.size);
        Field value;
        getDynamicSerialization()->deserializeBinary(value, buf, getFormatSettings());
        object[path] = value;
    }

    return object;
}

void ColumnObject::get(size_t n, Field & res) const
{
    res = (*this)[n];
}

std::pair<String, DataTypePtr> ColumnObject::getValueNameAndType(size_t n) const
{
    WriteBufferFromOwnString wb;
    wb << '{';

    bool first = true;

    for (const auto & [path, column] : typed_paths)
    {
        const auto & [value, type] = column->getValueNameAndType(n);

        if (first)
            first = false;
        else
            wb << ", ";

        writeDoubleQuoted(path, wb);
        wb << ": " << value;
    }

    for (const auto & [path, column] : dynamic_paths_ptrs)
    {
        /// Output only non-null values from dynamic paths. We cannot distinguish cases when
        /// dynamic path has Null value and when it's absent in the row and consider them equivalent.
        if (column->isNullAt(n))
            continue;

        const auto & [value, type] = column->getValueNameAndType(n);

        if (first)
            first = false;
        else
            wb << ", ";

        writeDoubleQuoted(path, wb);
        wb << ": " << value;
    }

    const auto & shared_data_offsets = getSharedDataOffsets();
    const auto [shared_paths, shared_values] = getSharedDataPathsAndValues();
    size_t start = shared_data_offsets[static_cast<ssize_t>(n) - 1];
    size_t end = shared_data_offsets[n];
    for (size_t i = start; i != end; ++i)
    {
        if (first)
            first = false;
        else
            wb << ", ";

        String path = shared_paths->getDataAt(i).toString();
        writeDoubleQuoted(path, wb);

        auto value_data = shared_values->getDataAt(i);
        ReadBufferFromMemory buf(value_data.data, value_data.size);
        auto decoded_type = decodeDataType(buf);

        if (isNothing(decoded_type))
        {
            wb << ": NULL";
            continue;
        }

        const auto column = decoded_type->createColumn();
        decoded_type->getDefaultSerialization()->deserializeBinary(*column, buf, getFormatSettings());

        const auto & [value, type] = column->getValueNameAndType(0);

        wb << ": " << value;
    }

    wb << "}";

    return {wb.str(), std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON)};
}

bool ColumnObject::isDefaultAt(size_t n) const
{
    for (const auto & [path, column] : typed_paths)
    {
        if (!column->isDefaultAt(n))
            return false;
    }

    for (const auto & [path, column] : dynamic_paths_ptrs)
    {
        if (!column->isDefaultAt(n))
            return false;
    }

    if (!shared_data->isDefaultAt(n))
        return false;

    return true;
}

StringRef ColumnObject::getDataAt(size_t) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getDataAt is not supported for {}", getName());
}

void ColumnObject::insertData(const char *, size_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method insertData is not supported for {}", getName());
}

ColumnDynamic * ColumnObject::tryToAddNewDynamicPath(std::string_view path)
{
    if (dynamic_paths.size() == max_dynamic_paths)
        return nullptr;

    auto new_dynamic_column = ColumnDynamic::create(max_dynamic_types);
    new_dynamic_column->reserve(shared_data->capacity());
    new_dynamic_column->insertManyDefaults(size());
    auto it = dynamic_paths.emplace(path, std::move(new_dynamic_column)).first;
    auto it_ptr = dynamic_paths_ptrs.emplace(path, assert_cast<ColumnDynamic *>(it->second.get())).first;
    sorted_dynamic_paths.insert(it->first);
    return it_ptr->second;
}

void ColumnObject::addNewDynamicPath(std::string_view path, MutableColumnPtr column)
{
    if (dynamic_paths.size() == max_dynamic_paths)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add new dynamic path as the limit ({}) on dynamic paths is reached", max_dynamic_paths);

    if (!empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Setting specific column for dynamic path is allowed only for empty object column");

    auto it = dynamic_paths.emplace(path, std::move(column)).first;
    dynamic_paths_ptrs.emplace(path, assert_cast<ColumnDynamic *>(it->second.get()));
    sorted_dynamic_paths.insert(it->first);
}

void ColumnObject::addNewDynamicPath(std::string_view path)
{
    if (!tryToAddNewDynamicPath(path))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add new dynamic path as the limit ({}) on dynamic paths is reached", max_dynamic_paths);
}

void ColumnObject::setMaxDynamicPaths(size_t max_dynamic_paths_)
{
    if (!empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Setting specific max_dynamic_paths parameter is allowed only for empty object column");

    max_dynamic_paths = max_dynamic_paths_;
}

void ColumnObject::setDynamicPaths(const std::vector<String> & paths)
{
    if (paths.size() > max_dynamic_paths)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot set dynamic paths to Object column, the number of paths ({}) exceeds the limit ({})", paths.size(), max_dynamic_paths);

    size_t size = this->size();
    for (const auto & path : paths)
    {
        auto new_dynamic_column = ColumnDynamic::create(max_dynamic_types);
        if (size)
            new_dynamic_column->insertManyDefaults(size);
        auto it = dynamic_paths.emplace(path, std::move(new_dynamic_column)).first;
        dynamic_paths_ptrs[path] = assert_cast<ColumnDynamic *>(it->second.get());
        sorted_dynamic_paths.insert(it->first);
    }
}

void ColumnObject::setDynamicPaths(const std::vector<std::pair<String, ColumnPtr>> & paths)
{
    if (paths.size() > max_dynamic_paths)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot set dynamic paths to Object column, the number of paths ({}) exceeds the limit ({})", paths.size(), max_dynamic_paths);

    for (const auto & [path, column] : paths)
    {
        auto it = dynamic_paths.emplace(path, column).first;
        dynamic_paths_ptrs[path] = assert_cast<ColumnDynamic *>(it->second.get());
        sorted_dynamic_paths.insert(it->first);
    }
}

void ColumnObject::insert(const Field & x)
{
    const auto & object = x.safeGet<Object>();
    auto & shared_data_offsets = getSharedDataOffsets();
    auto [shared_data_paths, shared_data_values] = getSharedDataPathsAndValues();
    size_t current_size = size();
    for (const auto & [path, value_field] : object)
    {
        if (auto typed_it = typed_paths.find(path); typed_it != typed_paths.end())
        {
            typed_it->second->insert(value_field);
        }
        else if (auto dynamic_it = dynamic_paths_ptrs.find(path); dynamic_it != dynamic_paths_ptrs.end())
        {
            dynamic_it->second->insert(value_field);
        }
        else if (auto * dynamic_path_column = tryToAddNewDynamicPath(path))
        {
            dynamic_path_column->insert(value_field);
        }
        /// We reached the limit on dynamic paths. Add this path to the common data if the value is not Null.
        /// (we cannot distinguish cases when path has Null value or is absent in the row and consider them equivalent).
        /// Object is actually std::map, so all paths are already sorted and we can add it right now.
        else if (!value_field.isNull())
        {
            shared_data_paths->insertData(path.data(), path.size());
            auto & shared_data_values_chars = shared_data_values->getChars();
            {
                WriteBufferFromVector<ColumnString::Chars> value_buf(shared_data_values_chars, AppendModeTag());
                getDynamicSerialization()->serializeBinary(value_field, value_buf, getFormatSettings());
            }
            shared_data_values_chars.push_back(0);
            shared_data_values->getOffsets().push_back(shared_data_values_chars.size());
        }
    }

    shared_data_offsets.push_back(shared_data_paths->size());

    /// Fill all remaining typed and dynamic paths with default values.
    for (auto & [_, column] : typed_paths)
    {
        if (column->size() == current_size)
            column->insertDefault();
    }

    for (auto & [_, column] : dynamic_paths_ptrs)
    {
        if (column->size() == current_size)
            column->insertDefault();
    }
}

bool ColumnObject::tryInsert(const Field & x)
{
    if (x.getType() != Field::Types::Which::Object)
        return false;

    const auto & object = x.safeGet<Object>();
    auto & shared_data_offsets = getSharedDataOffsets();
    auto [shared_data_paths, shared_data_values] = getSharedDataPathsAndValues();
    size_t prev_size = size();
    size_t prev_paths_size = shared_data_paths->size();
    size_t prev_values_size = shared_data_values->size();
    /// Save all newly added dynamic paths. In case of failure
    /// we should remove them.
    std::unordered_set<String> new_dynamic_paths;
    auto restore_sizes = [&]()
    {
        for (auto & [_, column] : typed_paths)
        {
            if (column->size() != prev_size)
                column->popBack(column->size() - prev_size);
        }

        /// Remove all newly added dynamic paths.
        for (const auto & path : new_dynamic_paths)
        {
            dynamic_paths_ptrs.erase(path);
            dynamic_paths.erase(path);
        }

        for (auto & [_, column] : dynamic_paths_ptrs)
        {
            if (column->size() != prev_size)
                column->popBack(column->size() - prev_size);
        }

        if (shared_data_paths->size() != prev_paths_size)  /// NOLINT(clang-analyzer-core.NullDereference)
            shared_data_paths->popBack(shared_data_paths->size() - prev_paths_size);
        if (shared_data_values->size() != prev_values_size)
            shared_data_values->popBack(shared_data_values->size() - prev_values_size);
    };

    for (const auto & [path, value_field] : object)
    {
        if (auto typed_it = typed_paths.find(path); typed_it != typed_paths.end())
        {
            if (!typed_it->second->tryInsert(value_field))
            {
                restore_sizes();
                return false;
            }
        }
        else if (auto dynamic_it = dynamic_paths_ptrs.find(path); dynamic_it != dynamic_paths_ptrs.end())
        {
            if (!dynamic_it->second->tryInsert(value_field))
            {
                restore_sizes();
                return false;
            }
        }
        else if (auto * dynamic_path_column = tryToAddNewDynamicPath(path))
        {
            if (!dynamic_path_column->tryInsert(value_field))
            {
                restore_sizes();
                return false;
            }
        }
        /// We reached the limit on dynamic paths. Add this path to the common data if the value is not Null.
        /// (we cannot distinguish cases when path has Null value or is absent in the row and consider them equivalent).
        /// Object is actually std::map, so all paths are already sorted and we can add it right now.
        else if (!value_field.isNull())
        {
            WriteBufferFromOwnString value_buf;
            getDynamicSerialization()->serializeBinary(value_field, value_buf, getFormatSettings());
            shared_data_paths->insertData(path.data(), path.size());
            shared_data_values->insertData(value_buf.str().data(), value_buf.str().size());
        }
    }

    shared_data_offsets.push_back(shared_data_paths->size());

    /// Fill all remaining typed and dynamic paths with default values.
    for (auto & [_, column] : typed_paths)
    {
        if (column->size() == prev_size)
            column->insertDefault();
    }

    for (auto & [_, column] : dynamic_paths_ptrs)
    {
        if (column->size() == prev_size)
            column->insertDefault();
    }

    return true;
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnObject::insertFrom(const IColumn & src, size_t n)
#else
void ColumnObject::doInsertFrom(const IColumn & src, size_t n)
#endif
{
    const auto & src_object_column = assert_cast<const ColumnObject &>(src);

    /// First, insert typed paths, they must be the same for both columns.
    for (const auto & [path, column] : src_object_column.typed_paths)
        typed_paths[path]->insertFrom(*column, n);

    /// Second, insert dynamic paths and extend them if needed.
    /// We can reach the limit of dynamic paths, and in this case
    /// the rest of dynamic paths will be inserted into shared data.
    std::vector<std::string_view> src_dynamic_paths_for_shared_data;
    for (const auto & [path, column] : src_object_column.dynamic_paths)
    {
        /// Check if we already have such dynamic path.
        if (auto it = dynamic_paths_ptrs.find(path); it != dynamic_paths_ptrs.end())
            it->second->insertFrom(*column, n);
        /// Try to add a new dynamic path.
        else if (auto * dynamic_path_column = tryToAddNewDynamicPath(path))
            dynamic_path_column->insertFrom(*column, n);
        /// Limit on dynamic paths is reached, add path to shared data later.
        else
            src_dynamic_paths_for_shared_data.push_back(path);
    }

    /// Finally, insert paths from shared data.
    insertFromSharedDataAndFillRemainingDynamicPaths(src_object_column, std::move(src_dynamic_paths_for_shared_data), n, 1);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnObject::insertRangeFrom(const IColumn & src, size_t start, size_t length)
#else
void ColumnObject::doInsertRangeFrom(const IColumn & src, size_t start, size_t length)
#endif
{
    /// TODO: try to parallelize doInsertRangeFrom over typed/dynamic paths if it makes sense.
    const auto & src_object_column = assert_cast<const ColumnObject &>(src);

    /// First, insert typed paths, they must be the same for both columns.
    for (const auto & [path, column] : src_object_column.typed_paths)
        typed_paths[path]->insertRangeFrom(*column, start, length);

    /// Second, insert dynamic paths and extend them if needed.
    /// We can reach the limit of dynamic paths, and in this case
    /// the rest of dynamic paths will be inserted into shared data.
    std::vector<std::string_view> src_dynamic_paths_for_shared_data;
    for (const auto & [path, column] : src_object_column.dynamic_paths)
    {
        /// Check if we already have such dynamic path.
        if (auto it = dynamic_paths_ptrs.find(path); it != dynamic_paths_ptrs.end())
            it->second->insertRangeFrom(*column, start, length);
        /// Try to add a new dynamic path.
        else if (auto * dynamic_path_column = tryToAddNewDynamicPath(path))
            dynamic_path_column->insertRangeFrom(*column, start, length);
        /// Limit on dynamic paths is reached, add path to shared data later.
        else
            src_dynamic_paths_for_shared_data.push_back(path);
    }

    /// Finally, insert paths from shared data.
    insertFromSharedDataAndFillRemainingDynamicPaths(src_object_column, std::move(src_dynamic_paths_for_shared_data), start, length);
}

void ColumnObject::insertFromSharedDataAndFillRemainingDynamicPaths(const DB::ColumnObject & src_object_column, std::vector<std::string_view> && src_dynamic_paths_for_shared_data, size_t start, size_t length)
{
    /// Paths in shared data are sorted, so paths from src_dynamic_paths_for_shared_data should be inserted properly
    /// to keep paths sorted. Let's sort them in advance.
    std::sort(src_dynamic_paths_for_shared_data.begin(), src_dynamic_paths_for_shared_data.end());

    /// Check if src object doesn't have any paths in shared data in specified range.
    const auto & src_shared_data_offsets = src_object_column.getSharedDataOffsets();
    if (src_shared_data_offsets[static_cast<ssize_t>(start) - 1] == src_shared_data_offsets[static_cast<ssize_t>(start) + length - 1])
    {
        size_t current_size = size();

        /// If no src dynamic columns should be inserted into shared data, insert defaults.
        if (src_dynamic_paths_for_shared_data.empty())
        {
            shared_data->insertManyDefaults(length);
        }
        /// Otherwise insert required src dynamic columns into shared data.
        else
        {
            const auto [shared_data_paths, shared_data_values] = getSharedDataPathsAndValues();
            auto & shared_data_offsets = getSharedDataOffsets();
            for (size_t i = start; i != start + length; ++i)
            {
                /// Paths in src_dynamic_paths_for_shared_data are already sorted.
                for (const auto path : src_dynamic_paths_for_shared_data)
                    serializePathAndValueIntoSharedData(shared_data_paths, shared_data_values, path, *src_object_column.dynamic_paths.find(path)->second, i);
                shared_data_offsets.push_back(shared_data_paths->size());
            }
        }

        /// Insert default values in all remaining dynamic paths.
        for (auto & [_, column] : dynamic_paths_ptrs)
        {
            if (column->size() == current_size)
                column->insertManyDefaults(length);
        }
        return;
    }

    /// Src object column contains some paths in shared data in specified range.
    /// Iterate over this range and insert all required paths into shared data or dynamic paths.
    const auto [src_shared_data_paths, src_shared_data_values] = src_object_column.getSharedDataPathsAndValues();
    const auto [shared_data_paths, shared_data_values] = getSharedDataPathsAndValues();
    auto & shared_data_offsets = getSharedDataOffsets();
    for (size_t row = start; row != start + length; ++row)
    {
        size_t current_size = shared_data_offsets.size();
        /// Use separate index to iterate over sorted src_dynamic_paths_for_shared_data.
        size_t src_dynamic_paths_for_shared_data_index = 0;
        size_t offset = src_shared_data_offsets[static_cast<ssize_t>(row) - 1];
        size_t end = src_shared_data_offsets[row];
        for (size_t i = offset; i != end; ++i)
        {
            auto path = src_shared_data_paths->getDataAt(i).toView();
            /// Check if we have this path in dynamic paths.
            if (auto it = dynamic_paths_ptrs.find(path); it != dynamic_paths_ptrs.end())
            {
                /// Deserialize binary value into dynamic column from shared data.
                deserializeValueFromSharedData(src_shared_data_values, i, *it->second);
            }
            else
            {
                /// Before inserting this path into shared data check if we need to
                /// insert dynamic paths from src_dynamic_paths_for_shared_data before.
                while (src_dynamic_paths_for_shared_data_index < src_dynamic_paths_for_shared_data.size()
                       && src_dynamic_paths_for_shared_data[src_dynamic_paths_for_shared_data_index] < path)
                {
                    const auto dynamic_path = src_dynamic_paths_for_shared_data[src_dynamic_paths_for_shared_data_index];
                    serializePathAndValueIntoSharedData(shared_data_paths, shared_data_values, dynamic_path, *src_object_column.dynamic_paths.find(dynamic_path)->second, row);
                    ++src_dynamic_paths_for_shared_data_index;
                }

                /// Insert path and value from src shared data to our shared data.
                shared_data_paths->insertFrom(*src_shared_data_paths, i);
                shared_data_values->insertFrom(*src_shared_data_values, i);
            }
        }

        /// Insert remaining dynamic paths from src_dynamic_paths_for_shared_data.
        for (; src_dynamic_paths_for_shared_data_index != src_dynamic_paths_for_shared_data.size(); ++src_dynamic_paths_for_shared_data_index)
        {
            const auto dynamic_path = src_dynamic_paths_for_shared_data[src_dynamic_paths_for_shared_data_index];
            serializePathAndValueIntoSharedData(shared_data_paths, shared_data_values, dynamic_path, *src_object_column.dynamic_paths.find(dynamic_path)->second, row);
        }

        shared_data_offsets.push_back(shared_data_paths->size());

        /// Insert default value in all remaining dynamic paths.
        for (auto & [_, column] : dynamic_paths_ptrs)
        {
            if (column->size() == current_size)
                column->insertDefault();
        }
    }
}

void ColumnObject::serializePathAndValueIntoSharedData(ColumnString * shared_data_paths, ColumnString * shared_data_values, std::string_view path, const IColumn & column, size_t n)
{
    /// Don't store Null values in shared data. We consider Null value equivalent to the absence
    /// of this path in the row because we cannot distinguish these 2 cases for dynamic paths.
    if (column.isNullAt(n))
        return;

    shared_data_paths->insertData(path.data(), path.size());
    auto & shared_data_values_chars = shared_data_values->getChars();
    {
        WriteBufferFromVector<ColumnString::Chars> value_buf(shared_data_values_chars, AppendModeTag());
        getDynamicSerialization()->serializeBinary(column, n, value_buf, getFormatSettings());
    }
    shared_data_values_chars.push_back(0);
    shared_data_values->getOffsets().push_back(shared_data_values_chars.size());
}

void ColumnObject::deserializeValueFromSharedData(const ColumnString * shared_data_values, size_t n, IColumn & column)
{
    auto value_data = shared_data_values->getDataAt(n);
    ReadBufferFromMemory buf(value_data.data, value_data.size);
    getDynamicSerialization()->deserializeBinary(column, buf, getFormatSettings());
}

void ColumnObject::insertDefault()
{
    for (auto & [_, column] : typed_paths)
        column->insertDefault();
    for (auto & [_, column] : dynamic_paths_ptrs)
        column->insertDefault();
    shared_data->insertDefault();
}

void ColumnObject::insertManyDefaults(size_t length)
{
    for (auto & [_, column] : typed_paths)
        column->insertManyDefaults(length);
    for (auto & [_, column] : dynamic_paths_ptrs)
        column->insertManyDefaults(length);
    shared_data->insertManyDefaults(length);
}

void ColumnObject::popBack(size_t n)
{
    for (auto & [_, column] : typed_paths)
        column->popBack(n);
    for (auto & [_, column] : dynamic_paths_ptrs)
        column->popBack(n);
    shared_data->popBack(n);
}

ColumnCheckpointPtr ColumnObject::getCheckpoint() const
{
    auto get_checkpoints = [](const auto & columns)
    {
        ColumnObjectCheckpoint::CheckpointsMap checkpoints;
        for (const auto & [name, column] : columns)
            checkpoints[name] = column->getCheckpoint();

        return checkpoints;
    };

    return std::make_shared<ColumnObjectCheckpoint>(size(), get_checkpoints(typed_paths), get_checkpoints(dynamic_paths_ptrs), shared_data->getCheckpoint());
}

void ColumnObject::updateCheckpoint(ColumnCheckpoint & checkpoint) const
{
    auto & object_checkpoint = assert_cast<ColumnObjectCheckpoint &>(checkpoint);

    auto update_checkpoints = [&](const auto & columns_map, auto & checkpoints_map)
    {
        for (const auto & [name, column] : columns_map)
        {
            auto & nested = checkpoints_map[name];
            if (!nested)
                nested = column->getCheckpoint();
            else
                column->updateCheckpoint(*nested);
        }
    };

    checkpoint.size = size();
    update_checkpoints(typed_paths, object_checkpoint.typed_paths);
    update_checkpoints(dynamic_paths, object_checkpoint.dynamic_paths);
    shared_data->updateCheckpoint(*object_checkpoint.shared_data);
}

void ColumnObject::rollback(const ColumnCheckpoint & checkpoint)
{
    const auto & object_checkpoint = assert_cast<const ColumnObjectCheckpoint &>(checkpoint);

    auto rollback_columns = [&](auto & columns_map, const auto & checkpoints_map, bool is_dynamic_paths)
    {
        NameSet names_to_remove;

        /// Rollback subcolumns and remove paths that were not in checkpoint.
        for (auto & [name, column] : columns_map)
        {
            auto it = checkpoints_map.find(name);
            if (it == checkpoints_map.end())
                names_to_remove.insert(name);
            else
                column->rollback(*it->second);
        }

        for (const auto & name : names_to_remove)
        {
            if (is_dynamic_paths)
            {
                dynamic_paths_ptrs.erase(name);
                sorted_dynamic_paths.erase(name);
            }

            columns_map.erase(name);
        }
    };

    rollback_columns(typed_paths, object_checkpoint.typed_paths, false);
    rollback_columns(dynamic_paths, object_checkpoint.dynamic_paths, true);
    shared_data->rollback(*object_checkpoint.shared_data);
}

StringRef ColumnObject::serializeValueIntoArena(size_t n, Arena & arena, const char *& begin) const
{
    StringRef res(begin, 0);
    /// First serialize values from typed paths in sorted order. They are the same for all instances of this column.
    for (auto path : sorted_typed_paths)
    {
        auto data_ref = typed_paths.find(path)->second->serializeValueIntoArena(n, arena, begin);
        res.data = data_ref.data - res.size;
        res.size += data_ref.size;
    }

    /// Second, serialize paths and values in bunary format from dynamic paths and shared data in sorted by path order.
    /// Calculate total number of paths to serialize and write it.
    const auto & shared_data_offsets = getSharedDataOffsets();
    size_t offset = shared_data_offsets[static_cast<ssize_t>(n) - 1];
    size_t end = shared_data_offsets[static_cast<ssize_t>(n)];
    size_t num_paths = (end - offset);
    /// Don't serialize Nulls from dynamic paths.
    for (const auto & [_, column] : dynamic_paths)
        num_paths += !column->isNullAt(n);

    char * pos = arena.allocContinue(sizeof(size_t), begin);
    memcpy(pos, &num_paths, sizeof(size_t));
    res.data = pos - res.size;
    res.size += sizeof(size_t);

    auto dynamic_paths_it = sorted_dynamic_paths.begin();
    auto [shared_data_paths, shared_data_values] = getSharedDataPathsAndValues();
    for (size_t i = offset; i != end; ++i)
    {
        auto path = shared_data_paths->getDataAt(i).toView();
        /// Paths in shared data are sorted. Serialize all paths from dynamic paths that go before this path in sorted order.
        while (dynamic_paths_it != sorted_dynamic_paths.end() && *dynamic_paths_it < path)
        {
            const auto * dynamic_column = dynamic_paths_ptrs.find(*dynamic_paths_it)->second;
            /// Don't serialize Nulls.
            if (!dynamic_column->isNullAt(n))
            {
                WriteBufferFromOwnString buf;
                getDynamicSerialization()->serializeBinary(*dynamic_column, n, buf, getFormatSettings());
                serializePathAndValueIntoArena(arena, begin, StringRef(*dynamic_paths_it), buf.str(), res);
            }
            ++dynamic_paths_it;
        }
        serializePathAndValueIntoArena(arena, begin, StringRef(path), shared_data_values->getDataAt(i), res);
    }

    /// Serialize all remaining paths in dynamic paths.
    for (; dynamic_paths_it != sorted_dynamic_paths.end(); ++dynamic_paths_it)
    {
        const auto * dynamic_column = dynamic_paths_ptrs.find(*dynamic_paths_it)->second;
        if (!dynamic_column->isNullAt(n))
        {
            WriteBufferFromOwnString buf;
            getDynamicSerialization()->serializeBinary(*dynamic_column, n, buf, getFormatSettings());
            serializePathAndValueIntoArena(arena, begin, StringRef(*dynamic_paths_it), buf.str(), res);
        }
    }

    return res;
}

void ColumnObject::serializePathAndValueIntoArena(DB::Arena & arena, const char *& begin, StringRef path, StringRef value, StringRef & res) const
{
    size_t value_size = value.size;
    size_t path_size = path.size;
    char * pos = arena.allocContinue(sizeof(size_t) + path_size + sizeof(size_t) + value_size, begin);
    memcpy(pos, &path_size, sizeof(size_t));
    memcpy(pos + sizeof(size_t), path.data, path_size);
    memcpy(pos + sizeof(size_t) + path_size, &value_size, sizeof(size_t));
    memcpy(pos + sizeof(size_t) + path_size + sizeof(size_t), value.data, value_size);
    res.data = pos - res.size;
    res.size += sizeof(size_t) + path_size + sizeof(size_t) + value_size;
}

const char * ColumnObject::deserializeAndInsertFromArena(const char * pos)
{
    size_t current_size = size();
    /// First deserialize typed paths. They come first.
    for (auto path : sorted_typed_paths)
        pos = typed_paths.find(path)->second->deserializeAndInsertFromArena(pos);

    /// Second deserialize all other paths and values and insert them into dynamic paths or shared data.
    auto num_paths = unalignedLoad<size_t>(pos);
    pos += sizeof(size_t);
    const auto [shared_data_paths, shared_data_values] = getSharedDataPathsAndValues();
    for (size_t i = 0; i != num_paths; ++i)
    {
        auto path_size = unalignedLoad<size_t>(pos);
        pos += sizeof(size_t);
        std::string_view path(pos, path_size);
        pos += path_size;
        /// Deserialize binary value and try to insert it to dynamic paths or shared data.
        auto value_size = unalignedLoad<size_t>(pos);
        pos += sizeof(size_t);
        std::string_view value(pos, value_size);
        pos += value_size;
        /// Check if we have this path in dynamic paths.
        if (auto dynamic_it = dynamic_paths.find(path); dynamic_it != dynamic_paths.end())
        {
            ReadBufferFromMemory buf(value.data(), value.size());
            getDynamicSerialization()->deserializeBinary(*dynamic_it->second, buf, getFormatSettings());
        }
        /// Try to add a new dynamic path.
        else if (auto * dynamic_path_column = tryToAddNewDynamicPath(path))
        {
            ReadBufferFromMemory buf(value.data(), value.size());
            getDynamicSerialization()->deserializeBinary(*dynamic_path_column, buf, getFormatSettings());
        }
        /// Limit on dynamic paths is reached, add this path to shared data.
        /// Serialized paths are sorted, so we can insert right away.
        else
        {
            shared_data_paths->insertData(path.data(), path.size());
            shared_data_values->insertData(value.data(), value.size());
        }
    }

    getSharedDataOffsets().push_back(shared_data_paths->size());

    /// Insert default value in all remaining  dynamic paths.
    for (auto & [_, column] : dynamic_paths_ptrs)
    {
        if (column->size() == current_size)
            column->insertDefault();
    }

    return pos;
}

const char * ColumnObject::skipSerializedInArena(const char * pos) const
{
    /// First, skip all values of typed paths;
    for (auto path : sorted_typed_paths)
        pos = typed_paths.find(path)->second->skipSerializedInArena(pos);

    /// Second, skip all other paths and values.
    auto num_paths = unalignedLoad<size_t>(pos);
    pos += sizeof(size_t);
    for (size_t i = 0; i != num_paths; ++i)
    {
        auto path_size = unalignedLoad<size_t>(pos);
        pos += sizeof(size_t);
        std::string_view path(pos, path_size);
        pos += path_size;
        auto value_size = unalignedLoad<size_t>(pos);
        pos += sizeof(size_t) + value_size;
    }

    return pos;
}

void ColumnObject::updateHashWithValue(size_t n, SipHash & hash) const
{
    for (auto path : sorted_typed_paths)
        typed_paths.find(path)->second->updateHashWithValue(n, hash);

    /// The hash of the object in row should not depend on the way we store paths (in dynamic paths or in shared data)
    /// and should be the same for the same objects. To support it we update hash with path and its value (if not null) in
    /// sorted by path order from both dynamic paths and shared data.
    const auto [shared_data_paths, shared_data_values] = getSharedDataPathsAndValues();
    const auto & shared_data_offsets = getSharedDataOffsets();
    size_t start = shared_data_offsets[static_cast<ssize_t>(n) - 1];
    size_t end = shared_data_offsets[static_cast<ssize_t>(n)];
    auto dynamic_paths_it = sorted_dynamic_paths.begin();
    for (size_t i = start; i != end; ++i)
    {
        auto path = shared_data_paths->getDataAt(i).toView();
        /// Paths in shared data are sorted. Update hash with all paths from dynamic paths that go before this path in sorted order.
        while (dynamic_paths_it != sorted_dynamic_paths.end() && *dynamic_paths_it < path)
        {
            const auto * dynamic_column = dynamic_paths_ptrs.find(*dynamic_paths_it)->second;
            if (!dynamic_column->isNullAt(n))
            {
                hash.update(*dynamic_paths_it);
                dynamic_column->updateHashWithValue(n, hash);
            }
            ++dynamic_paths_it;
        }

        /// Deserialize value in temporary column to get its hash.
        auto value = shared_data_values->getDataAt(i);
        ReadBufferFromMemory buf(value.data, value.size);
        auto tmp_column = ColumnDynamic::create();
        getDynamicSerialization()->deserializeBinary(*tmp_column, buf, getFormatSettings());
        hash.update(path);
        tmp_column->updateHashWithValue(0, hash);
    }

    /// Iterate over all remaining paths in dynamic paths.
    for (; dynamic_paths_it != sorted_dynamic_paths.end(); ++dynamic_paths_it)
    {
        const auto * dynamic_column = dynamic_paths_ptrs.find(*dynamic_paths_it)->second;
        if (!dynamic_column->isNullAt(n))
        {
            hash.update(*dynamic_paths_it);
            dynamic_column->updateHashWithValue(n, hash);
        }
    }
}

WeakHash32 ColumnObject::getWeakHash32() const
{
    WeakHash32 hash(size());
    for (const auto & [_, column] : typed_paths)
        hash.update(column->getWeakHash32());
    for (const auto & [_, column] : dynamic_paths_ptrs)
        hash.update(column->getWeakHash32());
    hash.update(shared_data->getWeakHash32());
    return hash;
}

void ColumnObject::updateHashFast(SipHash & hash) const
{
    for (const auto & [_, column] : typed_paths)
        column->updateHashFast(hash);
    for (const auto & [_, column] : dynamic_paths_ptrs)
        column->updateHashFast(hash);
    shared_data->updateHashFast(hash);
}

ColumnPtr ColumnObject::filter(const Filter & filt, ssize_t result_size_hint) const
{
    std::unordered_map<String, ColumnPtr> filtered_typed_paths;
    filtered_typed_paths.reserve(typed_paths.size());
    for (const auto & [path, column] : typed_paths)
        filtered_typed_paths[path] = column->filter(filt, result_size_hint);

    std::unordered_map<String, ColumnPtr> filtered_dynamic_paths;
    filtered_dynamic_paths.reserve(dynamic_paths_ptrs.size());
    for (const auto & [path, column] : dynamic_paths_ptrs)
        filtered_dynamic_paths[path] = column->filter(filt, result_size_hint);

    auto filtered_shared_data = shared_data->filter(filt, result_size_hint);
    return ColumnObject::create(filtered_typed_paths, filtered_dynamic_paths, filtered_shared_data, max_dynamic_paths, global_max_dynamic_paths, max_dynamic_types);
}

void ColumnObject::expand(const Filter & mask, bool inverted)
{
    for (auto & [_, column] : typed_paths)
        column->expand(mask, inverted);
    for (auto & [_, column] : dynamic_paths_ptrs)
        column->expand(mask, inverted);
    shared_data->expand(mask, inverted);
}

ColumnPtr ColumnObject::permute(const Permutation & perm, size_t limit) const
{
    std::unordered_map<String, ColumnPtr> permuted_typed_paths;
    permuted_typed_paths.reserve(typed_paths.size());
    for (const auto & [path, column] : typed_paths)
        permuted_typed_paths[path] = column->permute(perm, limit);

    std::unordered_map<String, ColumnPtr> permuted_dynamic_paths;
    permuted_dynamic_paths.reserve(dynamic_paths_ptrs.size());
    for (const auto & [path, column] : dynamic_paths_ptrs)
        permuted_dynamic_paths[path] = column->permute(perm, limit);

    auto permuted_shared_data = shared_data->permute(perm, limit);
    return ColumnObject::create(permuted_typed_paths, permuted_dynamic_paths, permuted_shared_data, max_dynamic_paths, global_max_dynamic_paths, max_dynamic_types);
}

ColumnPtr ColumnObject::index(const IColumn & indexes, size_t limit) const
{
    std::unordered_map<String, ColumnPtr> indexed_typed_paths;
    indexed_typed_paths.reserve(typed_paths.size());
    for (const auto & [path, column] : typed_paths)
        indexed_typed_paths[path] = column->index(indexes, limit);

    std::unordered_map<String, ColumnPtr> indexed_dynamic_paths;
    indexed_dynamic_paths.reserve(dynamic_paths_ptrs.size());
    for (const auto & [path, column] : dynamic_paths_ptrs)
        indexed_dynamic_paths[path] = column->index(indexes, limit);

    auto indexed_shared_data = shared_data->index(indexes, limit);
    return ColumnObject::create(indexed_typed_paths, indexed_dynamic_paths, indexed_shared_data, max_dynamic_paths, global_max_dynamic_paths, max_dynamic_types);
}

ColumnPtr ColumnObject::replicate(const Offsets & replicate_offsets) const
{
    std::unordered_map<String, ColumnPtr> replicated_typed_paths;
    replicated_typed_paths.reserve(typed_paths.size());
    for (const auto & [path, column] : typed_paths)
        replicated_typed_paths[path] = column->replicate(replicate_offsets);

    std::unordered_map<String, ColumnPtr> replicated_dynamic_paths;
    replicated_dynamic_paths.reserve(dynamic_paths_ptrs.size());
    for (const auto & [path, column] : dynamic_paths_ptrs)
        replicated_dynamic_paths[path] = column->replicate(replicate_offsets);

    auto replicated_shared_data = shared_data->replicate(replicate_offsets);
    return ColumnObject::create(replicated_typed_paths, replicated_dynamic_paths, replicated_shared_data, max_dynamic_paths, global_max_dynamic_paths, max_dynamic_types);
}

MutableColumns ColumnObject::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    std::vector<std::unordered_map<String, MutableColumnPtr>> scattered_typed_paths(num_columns);
    for (auto & typed_paths_ : scattered_typed_paths)
        typed_paths_.reserve(typed_paths.size());

    for (const auto & [path, column] : typed_paths)
    {
        auto scattered_columns = column->scatter(num_columns, selector);
        for (size_t i = 0; i != num_columns; ++i)
            scattered_typed_paths[i][path] = std::move(scattered_columns[i]);
    }

    std::vector<std::unordered_map<String, MutableColumnPtr>> scattered_dynamic_paths(num_columns);
    for (auto & dynamic_paths_ : scattered_dynamic_paths)
        dynamic_paths_.reserve(dynamic_paths_ptrs.size());

    for (const auto & [path, column] : dynamic_paths_ptrs)
    {
        auto scattered_columns = column->scatter(num_columns, selector);
        for (size_t i = 0; i != num_columns; ++i)
            scattered_dynamic_paths[i][path] = std::move(scattered_columns[i]);
    }

    auto scattered_shared_data_columns = shared_data->scatter(num_columns, selector);
    MutableColumns result_columns;
    result_columns.reserve(num_columns);
    for (size_t i = 0; i != num_columns; ++i)
        result_columns.emplace_back(ColumnObject::create(std::move(scattered_typed_paths[i]), std::move(scattered_dynamic_paths[i]), std::move(scattered_shared_data_columns[i]), max_dynamic_paths, global_max_dynamic_paths, max_dynamic_types));
    return result_columns;
}

struct ColumnObject::ComparatorBase
{
    const ColumnObject & parent;
    int nan_direction_hint;

    ComparatorBase(const ColumnObject & parent_, int nan_direction_hint_)
        : parent(parent_), nan_direction_hint(nan_direction_hint_)
    {
    }

    ALWAYS_INLINE int compare(size_t lhs, size_t rhs) const
    {
        int res = parent.compareAt(lhs, rhs, parent, nan_direction_hint);

        return res;
    }
};

void ColumnObject::getPermutation(PermutationSortDirection direction, PermutationSortStability stability,
                                  size_t limit, int nan_direction_hint, Permutation & res) const
{
    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorAscendingUnstable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorAscendingStable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorDescendingUnstable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorDescendingStable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
}

void ColumnObject::updatePermutation(PermutationSortDirection direction, PermutationSortStability stability,
                                     size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_ranges) const
{
    auto comparator_equal = ComparatorEqual(*this, nan_direction_hint);

    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingUnstable(*this, nan_direction_hint), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingStable(*this, nan_direction_hint), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingUnstable(*this, nan_direction_hint), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingStable(*this, nan_direction_hint), comparator_equal, DefaultSort(), DefaultPartialSort());
}

void ColumnObject::reserve(size_t n)
{
    for (auto & [_, column] : typed_paths)
        column->reserve(n);
    for (auto & [_, column] : dynamic_paths_ptrs)
        column->reserve(n);
    shared_data->reserve(n);
}

size_t ColumnObject::capacity() const
{
    return shared_data->capacity();
}

void ColumnObject::ensureOwnership()
{
    for (auto & [_, column] : typed_paths)
        column->ensureOwnership();
    for (auto & [_, column] : dynamic_paths_ptrs)
        column->ensureOwnership();
    shared_data->ensureOwnership();
}

size_t ColumnObject::byteSize() const
{
    size_t size = 0;
    for (const auto & [_, column] : typed_paths)
        size += column->byteSize();
    for (const auto & [_, column] : dynamic_paths_ptrs)
        size += column->byteSize();
    size += shared_data->byteSize();
    return size;
}

size_t ColumnObject::byteSizeAt(size_t n) const
{
    size_t size = 0;
    for (const auto & [_, column] : typed_paths)
        size += column->byteSizeAt(n);
    for (const auto & [_, column] : dynamic_paths_ptrs)
        size += column->byteSizeAt(n);
    size += shared_data->byteSizeAt(n);
    return size;
}

size_t ColumnObject::allocatedBytes() const
{
    size_t size = 0;
    for (const auto & [_, column] : typed_paths)
        size += column->allocatedBytes();
    for (const auto & [_, column] : dynamic_paths_ptrs)
        size += column->allocatedBytes();
    size += shared_data->allocatedBytes();
    return size;
}

void ColumnObject::protect()
{
    for (auto & [_, column] : typed_paths)
        column->protect();
    for (auto & [_, column] : dynamic_paths_ptrs)
        column->protect();
    shared_data->protect();
}

void ColumnObject::forEachMutableSubcolumn(DB::IColumn::MutableColumnCallback callback)
{
    for (auto & [_, column] : typed_paths)
        callback(column);
    for (auto & [path, column] : dynamic_paths)
    {
        callback(column);
        dynamic_paths_ptrs[path] = assert_cast<ColumnDynamic *>(column.get());
    }
    callback(shared_data);
}

void ColumnObject::forEachMutableSubcolumnRecursively(DB::IColumn::RecursiveMutableColumnCallback callback)
{
    for (auto & [_, column] : typed_paths)
    {
        callback(*column);
        column->forEachMutableSubcolumnRecursively(callback);
    }
    for (auto & [path, column] : dynamic_paths)
    {
        callback(*column);
        column->forEachMutableSubcolumnRecursively(callback);
        dynamic_paths_ptrs[path] = assert_cast<ColumnDynamic *>(column.get());
    }
    callback(*shared_data);
    shared_data->forEachMutableSubcolumnRecursively(callback);
}

void ColumnObject::forEachSubcolumn(DB::IColumn::ColumnCallback callback) const
{
    for (const auto & [_, column] : typed_paths)
        callback(column);
    for (const auto & [path, column] : dynamic_paths)
        callback(column);

    callback(shared_data);
}

void ColumnObject::forEachSubcolumnRecursively(DB::IColumn::RecursiveColumnCallback callback) const
{
    for (const auto & [_, column] : typed_paths)
    {
        callback(*column);
        column->forEachSubcolumnRecursively(callback);
    }
    for (const auto & [path, column] : dynamic_paths)
    {
        callback(*column);
        column->forEachSubcolumnRecursively(callback);
    }
    callback(*shared_data);
    shared_data->forEachSubcolumnRecursively(callback);
}

bool ColumnObject::structureEquals(const IColumn & rhs) const
{
    /// 2 Object columns have equal structure if they have the same typed paths and global_max_dynamic_paths/max_dynamic_types.
    const auto * rhs_object = typeid_cast<const ColumnObject *>(&rhs);
    if (!rhs_object || typed_paths.size() != rhs_object->typed_paths.size() || global_max_dynamic_paths != rhs_object->global_max_dynamic_paths || max_dynamic_types != rhs_object->max_dynamic_types)
        return false;

    for (const auto & [path, column] : typed_paths)
    {
        auto it = rhs_object->typed_paths.find(path);
        if (it == rhs_object->typed_paths.end() || !it->second->structureEquals(*column))
            return false;
    }

    return true;
}

ColumnPtr ColumnObject::compress(bool force_compression) const
{
    std::unordered_map<String, ColumnPtr> compressed_typed_paths;
    compressed_typed_paths.reserve(typed_paths.size());
    size_t byte_size = 0;
    for (const auto & [path, column] : typed_paths)
    {
        auto compressed_column = column->compress(force_compression);
        byte_size += compressed_column->byteSize();
        compressed_typed_paths[path] = std::move(compressed_column);
    }

    std::unordered_map<String, ColumnPtr> compressed_dynamic_paths;
    compressed_dynamic_paths.reserve(dynamic_paths_ptrs.size());
    for (const auto & [path, column] : dynamic_paths_ptrs)
    {
        auto compressed_column = column->compress(force_compression);
        byte_size += compressed_column->byteSize();
        compressed_dynamic_paths[path] = std::move(compressed_column);
    }

    auto compressed_shared_data = shared_data->compress(force_compression);
    byte_size += compressed_shared_data->byteSize();

    auto decompress =
        [my_compressed_typed_paths = std::move(compressed_typed_paths),
         my_compressed_dynamic_paths = std::move(compressed_dynamic_paths),
         my_compressed_shared_data = std::move(compressed_shared_data),
         my_max_dynamic_paths = max_dynamic_paths,
         my_global_max_dynamic_paths = global_max_dynamic_paths,
         my_max_dynamic_types = max_dynamic_types,
         my_statistics = statistics]() mutable
    {
        std::unordered_map<String, ColumnPtr> decompressed_typed_paths;
        decompressed_typed_paths.reserve(my_compressed_typed_paths.size());
        for (const auto & [path, column] : my_compressed_typed_paths)
            decompressed_typed_paths[path] = column->decompress();

        std::unordered_map<String, ColumnPtr> decompressed_dynamic_paths;
        decompressed_dynamic_paths.reserve(my_compressed_dynamic_paths.size());
        for (const auto & [path, column] : my_compressed_dynamic_paths)
            decompressed_dynamic_paths[path] = column->decompress();

        auto decompressed_shared_data = my_compressed_shared_data->decompress();
        return ColumnObject::create(decompressed_typed_paths, decompressed_dynamic_paths, decompressed_shared_data, my_max_dynamic_paths, my_global_max_dynamic_paths, my_max_dynamic_types, my_statistics);
    };

    return ColumnCompressed::create(size(), byte_size, decompress);
}

void ColumnObject::finalize()
{
    for (auto & [_, column] : typed_paths)
        column->finalize();
    for (auto & [_, column] : dynamic_paths_ptrs)
        column->finalize();
    shared_data->finalize();
}

bool ColumnObject::isFinalized() const
{
    bool finalized = true;
    for (const auto & [_, column] : typed_paths)
        finalized &= column->isFinalized();
    for (const auto & [_, column] : dynamic_paths_ptrs)
        finalized &= column->isFinalized();
    finalized &= shared_data->isFinalized();
    return finalized;
}

void ColumnObject::getExtremes(DB::Field & min, DB::Field & max) const
{
    if (empty())
    {
        min = Object();
        max = Object();
    }
    else
    {
        get(0, min);
        get(0, max);
    }
}

void ColumnObject::prepareForSquashing(const std::vector<ColumnPtr> & source_columns, size_t factor)
{
    if (source_columns.empty())
        return;

    /// Dynamic paths of source Object columns may differ.
    /// We want to preallocate memory for all dynamic paths we will have after squashing.
    /// It may happen that the total number of dynamic paths in source columns will
    /// exceed the limit, in this case we will choose the most frequent paths.
    std::unordered_map<String, size_t> path_to_total_number_of_non_null_values;

    auto add_dynamic_paths = [&](const ColumnObject & source_object)
    {
        for (const auto & [path, dynamic_column_ptr] : source_object.dynamic_paths_ptrs)
        {
            auto it = path_to_total_number_of_non_null_values.find(path);
            if (it == path_to_total_number_of_non_null_values.end())
                it = path_to_total_number_of_non_null_values.emplace(path, 0).first;
            it->second += (dynamic_column_ptr->size() - dynamic_column_ptr->getNumberOfDefaultRows());
        }
    };

    for (const auto & source_column : source_columns)
        add_dynamic_paths(assert_cast<const ColumnObject &>(*source_column));

    /// Add dynamic paths from this object column.
    add_dynamic_paths(*this);

    /// It might happen that current max_dynamic_paths is less then global_max_dynamic_paths
    /// but the shared data is empty. For example if this block was deserialized from Native format.
    /// In this case we should set max_dynamic_paths = global_max_dynamic_paths, so during squashing we
    /// will insert new types to SharedVariant only when the global limit is reached.
    if (getSharedDataPathsAndValues().first->empty())
        max_dynamic_paths = global_max_dynamic_paths;

    /// Check if the number of all dynamic paths exceeds the limit.
    if (path_to_total_number_of_non_null_values.size() > max_dynamic_paths)
    {
        /// We want to keep the most frequent paths in the resulting object column.
        /// Sort paths by total number of non null values.
        /// Don't include paths from current column as we cannot change them.
        std::vector<std::pair<size_t, std::string_view>> paths_with_sizes;
        paths_with_sizes.reserve(path_to_total_number_of_non_null_values.size() - dynamic_paths.size());
        for (const auto & [path, size] : path_to_total_number_of_non_null_values)
        {
            if (!dynamic_paths.contains(path))
                paths_with_sizes.emplace_back(size, path);
        }
        std::sort(paths_with_sizes.begin(), paths_with_sizes.end(), std::greater());

        /// Fill dynamic_paths with first paths in sorted list until we reach the limit.
        size_t paths_to_add = max_dynamic_paths - dynamic_paths.size();
        for (size_t i = 0; i != paths_to_add; ++i)
            addNewDynamicPath(paths_with_sizes[i].second);
    }
    /// Otherwise keep all paths.
    else
    {
        /// Create columns for new dynamic paths.
        for (const auto & [path, _] : path_to_total_number_of_non_null_values)
        {
            if (!dynamic_paths.contains(path))
                addNewDynamicPath(path);
        }
    }

    /// Now current object column has all resulting dynamic paths and we can call
    /// prepareForSquashing on them to preallocate the memory.
    /// Also we can preallocate memory for dynamic paths and shared data.
    Columns shared_data_source_columns;
    shared_data_source_columns.reserve(source_columns.size());
    std::unordered_map<String, Columns> typed_paths_source_columns;
    typed_paths_source_columns.reserve(typed_paths.size());
    std::unordered_map<String, Columns> dynamic_paths_source_columns;
    dynamic_paths_source_columns.reserve(dynamic_paths.size());

    for (const auto & [path, column] : typed_paths)
        typed_paths_source_columns[path].reserve(source_columns.size());

    for (const auto & [path, column] : dynamic_paths)
        dynamic_paths_source_columns[path].reserve(source_columns.size());

    size_t total_size = 0;
    for (const auto & source_column : source_columns)
    {
        const auto & source_object_column = assert_cast<const ColumnObject &>(*source_column);
        total_size += source_object_column.size();
        shared_data_source_columns.push_back(source_object_column.shared_data);

        for (const auto & [path, column] : source_object_column.typed_paths)
            typed_paths_source_columns.at(path).push_back(column);

        for (const auto & [path, column] : source_object_column.dynamic_paths)
        {
            if (dynamic_paths.contains(path))
                dynamic_paths_source_columns.at(path).push_back(column);
        }
    }

    shared_data->prepareForSquashing(shared_data_source_columns, factor);

    for (const auto & [path, source_typed_columns] : typed_paths_source_columns)
        typed_paths[path]->prepareForSquashing(source_typed_columns, factor);

    for (const auto & [path, source_dynamic_columns] : dynamic_paths_source_columns)
    {
        /// ColumnDynamic::prepareForSquashing may not preallocate enough memory for discriminators and offsets
        /// because source columns may not have this dynamic path (and so dynamic columns filled with nulls).
        /// For this reason we first call ColumnDynamic::reserve with resulting size to preallocate memory for
        /// discriminators and offsets and ColumnDynamic::prepareVariantsForSquashing to preallocate memory
        /// for all variants inside Dynamic.
        dynamic_paths_ptrs[path]->reserve(total_size);
        dynamic_paths_ptrs[path]->prepareVariantsForSquashing(source_dynamic_columns, factor);
    }
}

bool ColumnObject::dynamicStructureEquals(const IColumn & rhs) const
{
    const auto * rhs_object = typeid_cast<const ColumnObject *>(&rhs);
    if (!rhs_object || typed_paths.size() != rhs_object->typed_paths.size()
        || global_max_dynamic_paths != rhs_object->global_max_dynamic_paths || max_dynamic_types != rhs_object->max_dynamic_types
        || dynamic_paths.size() != rhs_object->dynamic_paths.size())
        return false;

    for (const auto & [path, column] : typed_paths)
    {
        auto it = rhs_object->typed_paths.find(path);
        if (it == rhs_object->typed_paths.end() || !it->second->dynamicStructureEquals(*column))
            return false;
    }

    for (const auto & [path, column] : dynamic_paths)
    {
        auto it = rhs_object->dynamic_paths.find(path);
        if (it == rhs_object->dynamic_paths.end() || !it->second->dynamicStructureEquals(*column))
            return false;
    }

    return true;
}

void ColumnObject::takeDynamicStructureFromSourceColumns(const DB::Columns & source_columns)
{
    if (!empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "takeDynamicStructureFromSourceColumns should be called only on empty Object column");

    /// During serialization of Object column in MergeTree all Object columns
    /// in single part must have the same structure (the same dynamic paths). During merge
    /// resulting column is constructed by inserting from source columns,
    /// but it may happen that resulting column doesn't have rows from all source parts
    /// but only from subset of them, and as a result some dynamic paths could be missing
    /// and structures of resulting column may differ.
    /// To solve this problem, before merge we create empty resulting column and use this method
    /// to take dynamic structure from all source columns even if we won't insert
    /// rows from some of them.

    /// We want to construct resulting set of dynamic paths with paths that have least number of null values in source columns
    /// and insert the rest paths into shared data if we exceed the limit of dynamic paths.
    /// First, collect all dynamic paths from all source columns and calculate total number of non-null values.
    std::unordered_map<String, size_t> path_to_total_number_of_non_null_values;
    for (const auto & source_column : source_columns)
    {
        const auto & source_object = assert_cast<const ColumnObject &>(*source_column);
        /// During deserialization from MergeTree we will have statistics from the whole
        /// data part with number of non null values for each dynamic path.
        const auto & source_statistics = source_object.getStatistics();
        for (const auto & [path, column_ptr] : source_object.dynamic_paths_ptrs)
        {
            auto it = path_to_total_number_of_non_null_values.find(path);
            if (it == path_to_total_number_of_non_null_values.end())
                it = path_to_total_number_of_non_null_values.emplace(path, 0).first;
            size_t size = column_ptr->size() - column_ptr->getNumberOfDefaultRows();
            if (source_statistics)
            {
                auto statistics_it = source_statistics->dynamic_paths_statistics.find(path);
                if (statistics_it != source_statistics->dynamic_paths_statistics.end())
                    size = statistics_it->second;
            }
            it->second += size;
        }

        /// Add paths from shared data statistics. It can helo extracting frequent paths
        /// from shared data to dynamic paths.
        if (source_statistics)
        {
            for (const auto & [path, size] : source_statistics->shared_data_paths_statistics)
            {
                auto it = path_to_total_number_of_non_null_values.find(path);
                if (it == path_to_total_number_of_non_null_values.end())
                    it = path_to_total_number_of_non_null_values.emplace(path, 0).first;
                it->second += size;
            }
        }
    }

    /// Reset current state.
    dynamic_paths.clear();
    dynamic_paths_ptrs.clear();
    sorted_dynamic_paths.clear();
    max_dynamic_paths = global_max_dynamic_paths;
    Statistics new_statistics(Statistics::Source::MERGE);

    /// Check if the number of all dynamic paths exceeds the limit.
    if (path_to_total_number_of_non_null_values.size() > max_dynamic_paths)
    {
        /// Sort paths by total number of non null values.
        std::vector<std::pair<size_t, std::string_view>> paths_with_sizes;
        paths_with_sizes.reserve(path_to_total_number_of_non_null_values.size());
        for (const auto & [path, size] : path_to_total_number_of_non_null_values)
            paths_with_sizes.emplace_back(size, path);
        std::sort(paths_with_sizes.begin(), paths_with_sizes.end(), std::greater());

        /// Fill dynamic_paths with first max_dynamic_paths paths in sorted list.
        for (const auto & [size, path] : paths_with_sizes)
        {
            if (dynamic_paths.size() < max_dynamic_paths)
            {
                auto it = dynamic_paths.emplace(path, ColumnDynamic::create(max_dynamic_types)).first;
                dynamic_paths_ptrs.emplace(path, assert_cast<ColumnDynamic *>(it->second.get()));
                sorted_dynamic_paths.insert(it->first);
            }
            /// Add all remaining paths into shared data statistics until we reach its max size;
            else if (new_statistics.shared_data_paths_statistics.size() < Statistics::MAX_SHARED_DATA_STATISTICS_SIZE)
            {
                new_statistics.shared_data_paths_statistics.emplace(path, size);
            }
        }
    }
    /// Use all dynamic paths from all source columns.
    else
    {
        for (const auto & [path, _] : path_to_total_number_of_non_null_values)
        {
            auto it = dynamic_paths.emplace(path, ColumnDynamic::create(max_dynamic_types)).first;
            dynamic_paths_ptrs[path] = assert_cast<ColumnDynamic *>(it->second.get());
            sorted_dynamic_paths.insert(it->first);
        }
    }

    /// Fill statistics for the merged part.
    for (const auto & [path, _] : dynamic_paths)
        new_statistics.dynamic_paths_statistics[path] = path_to_total_number_of_non_null_values[path];
    statistics = std::make_shared<const Statistics>(std::move(new_statistics));

    /// Set max_dynamic_paths to the number of selected dynamic paths.
    /// It's needed to avoid adding new unexpected dynamic paths during inserts into this column during merge.
    max_dynamic_paths = dynamic_paths.size();

    /// Now we have the resulting set of dynamic paths that will be used in all merged columns.
    /// As we use Dynamic column for dynamic paths, we should call takeDynamicStructureFromSourceColumns
    /// on all resulting dynamic columns.
    for (auto & [path, column] : dynamic_paths)
    {
        Columns dynamic_path_source_columns;
        for (const auto & source_column : source_columns)
        {
            const auto & source_object = assert_cast<const ColumnObject &>(*source_column);
            auto it = source_object.dynamic_paths.find(path);
            if (it != source_object.dynamic_paths.end())
                dynamic_path_source_columns.push_back(it->second);
        }
        column->takeDynamicStructureFromSourceColumns(dynamic_path_source_columns);
    }

    /// Typed paths also can contain types with dynamic structure.
    for (auto & [path, column] : typed_paths)
    {
        Columns typed_path_source_columns;
        typed_path_source_columns.reserve(source_columns.size());
        for (const auto & source_column : source_columns)
            typed_path_source_columns.push_back(assert_cast<const ColumnObject &>(*source_column).typed_paths.at(path));
        column->takeDynamicStructureFromSourceColumns(typed_path_source_columns);
    }
}

size_t ColumnObject::findPathLowerBoundInSharedData(StringRef path, const ColumnString & shared_data_paths, size_t start, size_t end)
{
    /// Simple random access iterator over values in ColumnString in specified range.
    class Iterator
    {
    public:
        using difference_type = size_t;
        using value_type = StringRef;
        using iterator_category = std::random_access_iterator_tag;
        using pointer = StringRef*;
        using reference = StringRef&;

        Iterator() = delete;
        Iterator(const ColumnString * data_, size_t index_) : data(data_), index(index_) {}
        Iterator(const Iterator & rhs) = default;
        Iterator & operator=(const Iterator & rhs) = default;
        inline Iterator& operator+=(difference_type rhs) { index += rhs; return *this;}
        inline StringRef operator*() const {return data->getDataAt(index);}

        inline Iterator& operator++() { ++index; return *this; }
        inline difference_type operator-(const Iterator & rhs) const {return index - rhs.index; }

        const ColumnString * data;
        size_t index;
    };

    Iterator start_it(&shared_data_paths, start);
    Iterator end_it(&shared_data_paths, end);
    auto it = std::lower_bound(start_it, end_it, path);
    return it.index;
}

void ColumnObject::fillPathColumnFromSharedData(IColumn & path_column, StringRef path, const ColumnPtr & shared_data_column, size_t start, size_t end)
{
    const auto & shared_data_array = assert_cast<const ColumnArray &>(*shared_data_column);
    const auto & shared_data_offsets = shared_data_array.getOffsets();
    size_t first_offset = shared_data_offsets[static_cast<ssize_t>(start) - 1];
    size_t last_offset = shared_data_offsets[static_cast<ssize_t>(end) - 1];
    /// Check if we have at least one row with data.
    if (first_offset == last_offset)
    {
        path_column.insertManyDefaults(end - start);
        return;
    }

    const auto & shared_data_tuple = assert_cast<const ColumnTuple &>(shared_data_array.getData());
    const auto & shared_data_paths = assert_cast<const ColumnString &>(shared_data_tuple.getColumn(0));
    const auto & shared_data_values = assert_cast<const ColumnString &>(shared_data_tuple.getColumn(1));
    const auto & dynamic_serialization = getDynamicSerialization();
    for (size_t i = start; i != end; ++i)
    {
        size_t paths_start = shared_data_offsets[static_cast<ssize_t>(i) - 1];
        size_t paths_end = shared_data_offsets[static_cast<ssize_t>(i)];
        auto lower_bound_path_index = ColumnObject::findPathLowerBoundInSharedData(path, shared_data_paths, paths_start, paths_end);
        if (lower_bound_path_index != paths_end && shared_data_paths.getDataAt(lower_bound_path_index) == path)
        {
            auto value_data = shared_data_values.getDataAt(lower_bound_path_index);
            ReadBufferFromMemory buf(value_data.data, value_data.size);
            dynamic_serialization->deserializeBinary(path_column, buf, getFormatSettings());
        }
        else
        {
            path_column.insertDefault();
        }
    }
}

/// Class that allows to iterate over paths inside single row in ColumnObject in sorted order.
class ColumnObject::SortedPathsIterator
{
public:
    SortedPathsIterator(const ColumnObject & column_object_, size_t row_)
        : column_object(column_object_)
        , typed_paths_it(column_object.sorted_typed_paths.begin())
        , typed_paths_end(column_object.sorted_typed_paths.end())
        , dynamic_paths_it(column_object.sorted_dynamic_paths.begin())
        , dynamic_paths_end(column_object.sorted_dynamic_paths.end())
        , row(row_)
    {
        std::tie(shared_data_paths, shared_data_values) = column_object.getSharedDataPathsAndValues();
        const auto & shared_data_offsets = column_object.getSharedDataOffsets();
        shared_data_it = shared_data_offsets[row - 1];
        shared_data_end = shared_data_offsets[row];
        setCurrentPath();
    }

    void next()
    {
        switch (current_path_type)
        {
            case PathType::DYNAMIC:
                ++dynamic_paths_it;
                break;
            case PathType::TYPED:
                ++typed_paths_it;
                break;
            case PathType::SHARED_DATA:
                ++shared_data_it;
                break;
        }

        setCurrentPath();
    }

    /// Compare paths and values of 2 iterators.
    /// Returns -1, 0, 1 if this iterator is less, equal or greater than rhs.
    int compare(const SortedPathsIterator & rhs, int nan_direction_hint) const
    {
        /// First, compare paths as strings.
        auto path = getCurrentPath();
        auto rhs_path = rhs.getCurrentPath();
        if (path != rhs_path)
            return path < rhs_path ? -1 : 1;

        /// If paths are equal, compare their values.
        auto [column, n] = getCurrentPathColumnAndRow();
        auto [rhs_column, rhs_n] = rhs.getCurrentPathColumnAndRow();
        return column->compareAt(n, rhs_n, *rhs_column, nan_direction_hint);
    }

    bool end()
    {
        return typed_paths_it == typed_paths_end && dynamic_paths_it == dynamic_paths_end && shared_data_it == shared_data_end;
    }

private:
    void setCurrentPath()
    {
        /// We have 3 sorted lists of paths - typed paths, dynamic paths and paths in shared data.
        /// We store iterators for each of these lists. Here we try to find the iterator with the lexicographically smallest path.

        /// Null in dynamic path is considered as absence of this path.
        while (dynamic_paths_it != dynamic_paths_end && column_object.dynamic_paths.find(*dynamic_paths_it)->second->isNullAt(row))
            ++dynamic_paths_it;

        std::array<std::pair<PathType, std::optional<std::string_view>>, 3> paths{
            std::pair{PathType::TYPED, typed_paths_it == typed_paths_end ? std::nullopt : std::optional<std::string_view>(*typed_paths_it)},
            std::pair{PathType::DYNAMIC, dynamic_paths_it == dynamic_paths_end ? std::nullopt : std::optional<std::string_view>(*dynamic_paths_it)},
            std::pair{
                PathType::SHARED_DATA, shared_data_it == shared_data_end ? std::nullopt : std::optional<std::string_view>(shared_data_paths->getDataAt(shared_data_it).toView())},
        };

        auto min_path = std::ranges::min(
            paths,
            [](auto first_path, auto second_path)
            {
                if (!second_path.second)
                    return true;

                if (!first_path.second)
                    return false;

                return first_path.second < second_path.second;
            });

        if (min_path.second)
            current_path_type = min_path.first;
    }

    std::string_view getCurrentPath() const
    {
        switch (current_path_type)
        {
            case PathType::DYNAMIC:
                return *dynamic_paths_it;
            case PathType::TYPED:
                return *typed_paths_it;
            case PathType::SHARED_DATA:
                return shared_data_paths->getDataAt(shared_data_it).toView();
        }
    }

    std::pair<ColumnPtr, size_t> getCurrentPathColumnAndRow() const
    {
        switch (current_path_type)
        {
            case PathType::DYNAMIC:
                return {column_object.dynamic_paths.find(*dynamic_paths_it)->second, row};
            case PathType::TYPED:
                return {column_object.typed_paths.find(*typed_paths_it)->second, row};
            case PathType::SHARED_DATA:
            {
                auto tmp_column = ColumnDynamic::create();
                ColumnObject::deserializeValueFromSharedData(shared_data_values, shared_data_it, *tmp_column);
                return {std::move(tmp_column), 0};
            }
        }
    }

    enum class PathType
    {
        TYPED,
        DYNAMIC,
        SHARED_DATA,
    };

    const ColumnObject & column_object;
    decltype(column_object.sorted_typed_paths)::const_iterator typed_paths_it;
    decltype(column_object.sorted_typed_paths)::const_iterator typed_paths_end;
    decltype(column_object.sorted_dynamic_paths)::const_iterator dynamic_paths_it;
    decltype(column_object.sorted_dynamic_paths)::const_iterator dynamic_paths_end;
    size_t shared_data_it;
    size_t shared_data_end;
    const ColumnString * shared_data_paths;
    const ColumnString * shared_data_values;
    PathType current_path_type;
    size_t row;
};

#if !defined(DEBUG_OR_SANITIZER_BUILD)
int ColumnObject::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#else
int ColumnObject::doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#endif
{
    /// We compare objects lexicographically like maps.
    const ColumnObject & rhs_object = assert_cast<const ColumnObject &>(rhs);
    /// Iterate over paths in both columns in sorted order.
    SortedPathsIterator it(*this, n);
    SortedPathsIterator rhs_it(rhs_object, m);

    while (!it.end() && !rhs_it.end())
    {
        auto cmp = it.compare(rhs_it, nan_direction_hint);
        if (cmp)
            return cmp;
        it.next();
        rhs_it.next();
    }

    if (it.end() && rhs_it.end())
        return 0;

    if (it.end())
        return -1;
    return 1;
}

}
