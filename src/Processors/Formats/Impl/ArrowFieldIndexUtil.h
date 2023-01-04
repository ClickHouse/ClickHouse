#pragma once
#include "config.h"
#if USE_PARQUET || USE_ORC
#include <map>
#include <Core/Block.h>
#include "DataTypes/Serializations/ISerialization.h"
namespace arrow
{
class Schema;
class DataType;
class Field;
}
namespace DB
{
class ArrowFieldIndexUtil
{
public:
    /// For orc format, nested_type_has_index_ = true.
    explicit ArrowFieldIndexUtil(bool ignore_case_, bool nested_type_has_index_, bool allow_missing_columns_)
        : ignore_case(ignore_case_)
        , nested_type_has_index(nested_type_has_index_)
        , allow_missing_columns(allow_missing_columns_)
    {
    }
    ~ArrowFieldIndexUtil() = default;

    std::map<std::string, std::pair<int, int>>
        calculateFieldIndices(const arrow::Schema & schema);

    std::vector<int> findRequiredIndices(const Block & header, const arrow::Schema & schema);

    size_t countIndicesForType(std::shared_ptr<arrow::DataType> type);

private:
    bool ignore_case;
    bool nested_type_has_index;
    bool allow_missing_columns;
    void calculateFieldIndices(const arrow::Field & field,
        std::string field_name,
        int & current_start_index,
        std::map<std::string, std::pair<int, int>> & result, const std::string & name_prefix = "");
};
}
#endif

