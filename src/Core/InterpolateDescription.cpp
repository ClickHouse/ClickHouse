#include <Core/Block.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Core/InterpolateDescription.h>
#include <Interpreters/convertFieldToType.h>

namespace DB
{

void dumpInterpolateDescription(const InterpolateDescription & description, const Block & /*header*/, WriteBuffer & out)
{
    bool first = true;

    for (const auto & desc : description)
    {
        if (!first)
            out << ", ";
        first = false;

        if (desc.column.name.empty())
            out << "?";
        else
            out << desc.column.name;
    }
}

void InterpolateColumnDescription::interpolate(Field & field) const
{
    if (field.isNull())
        return;
    Block expr_columns;
    Field column_field = convertFieldToType(field, *column.type.get());
    expr_columns.insert({column.type->createColumnConst(1, column_field), column.type, column.name});
    actions->execute(expr_columns);
    field = convertFieldToType((*expr_columns.getByPosition(0).column)[0], *column.type.get());
}

void InterpolateColumnDescription::explain(JSONBuilder::JSONMap & map, const Block & /*header*/) const
{
    map.add("Column", column.name);
}

std::string dumpInterpolateDescription(const InterpolateDescription & description)
{
    WriteBufferFromOwnString wb;
    dumpInterpolateDescription(description, Block{}, wb);
    return wb.str();
}

JSONBuilder::ItemPtr explainInterpolateDescription(const InterpolateDescription & description, const Block & header)
{
    auto json_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & descr : description)
    {
        auto json_map = std::make_unique<JSONBuilder::JSONMap>();
        descr.explain(*json_map, header);
        json_array->add(std::move(json_map));
    }

    return json_array;
}

}
