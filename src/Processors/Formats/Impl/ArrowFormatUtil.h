#pragma once
#include <map>
#include <Core/Block.h>
#include <arrow/api.h>
#include <arrow/type.h>
#include "DataTypes/Serializations/ISerialization.h"
namespace DB
{
class ArrowFormatUtil
{
public:
    explicit ArrowFormatUtil(bool ignore_case_, bool import_nested_, bool nested_type_has_index_)
        : ignore_case(ignore_case_)
        , import_nested(import_nested_)
        , nested_type_has_index(nested_type_has_index_){}
    ~ArrowFormatUtil() = default;

    std::map<std::string, std::pair<int, int>>
        calculateFieldIndices(const arrow::Schema & schema);

    std::vector<int> findRequiredIndices(const Block & header, const arrow::Schema & schema);

    size_t countIndicesForType(std::shared_ptr<arrow::DataType> type);

private:
    bool ignore_case;
    bool import_nested;
    bool nested_type_has_index;
    void calculateFieldIndices(const arrow::Field & field,
        int & current_start_index,
        std::map<std::string, std::pair<int, int>> & result, const std::string & name_prefix = "");
};
}
