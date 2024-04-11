#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>

#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <Parsers/ASTStreamSettings.h>

namespace DB
{

static String cursorToString(const Map & cursor)
{
    WriteBufferFromOwnString wb;

    wb << '{';

    for (size_t i = 0; i < cursor.size(); ++i)
    {
        if (i > 0)
            wb << ", ";

        const auto & tuple = cursor[i].safeGet<Tuple>();
        const auto & dotted_path = tuple.at(0).safeGet<String>();
        const auto & value = tuple.at(1).get<UInt64>();

        wb << quoteString(dotted_path) << ": " << value;
    }

    wb << '}';

    return wb.str();
}

ASTStreamSettings::ASTStreamSettings(StreamReadingStage stage_, std::optional<String> keeper_key_, std::optional<Map> collapsed_tree_)
    : stage{std::move(stage_)}, keeper_key{keeper_key_}, collapsed_tree{std::move(collapsed_tree_)}
{
}

void ASTStreamSettings::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    format.ostr << (format.hilite ? hilite_keyword : "") << "STREAM" << (format.hilite ? hilite_none : "");

    if (stage == StreamReadingStage::TailOnly)
        format.ostr << (format.hilite ? hilite_keyword : "") << " TAIL" << (format.hilite ? hilite_none : "");
    else if (collapsed_tree.has_value() || keeper_key.has_value())
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << " CURSOR" << (format.hilite ? hilite_none : "");

        if (keeper_key.has_value())
            format.ostr << " " << quoteString(keeper_key.value());

        if (collapsed_tree.has_value())
            format.ostr << " " << cursorToString(collapsed_tree.value());
    }
}

}
