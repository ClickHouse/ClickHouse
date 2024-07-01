#pragma once
#include <DataTypes/IDataType.h>
#include <Columns/IColumn.h>

namespace DB
{

template <typename Element>
struct JSONExtractTree
{
    class Node
    {
    public:
        Node() = default;
        virtual ~Node() = default;
        virtual bool insertResultToColumn(IColumn &, const Element &) = 0;
    };

    struct Settings
    {
        bool convert_bool_to_integer = true;
        bool type_json_infer_numbers_from_strings = true;
        bool type_json_infer_date = true;
        bool type_json_infer_datetime = true;
        bool type_json_infer_ipv4 = true;
        bool type_json_infer_ipv6 = true;
        bool type_json_infer_uuid = true;
        bool insert_null_as_default = true;
    };

    static std::unique_ptr<Node> build(const DataTypePtr & type, const Settings & settings, const char * source_for_exception_message);
};

template <typename Element>
void elementToString(const Element & element, WriteBuffer & buf);

}
