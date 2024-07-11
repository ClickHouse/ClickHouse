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

ASTStreamSettings::ASTStreamSettings(StreamSettings settings_)
    : settings{std::move(settings_)}
{
}

void ASTStreamSettings::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    format.ostr << (format.hilite ? hilite_keyword : "") << "STREAM" << (format.hilite ? hilite_none : "");

    if (settings.stage == StreamReadingStage::TailOnly)
        format.ostr << (format.hilite ? hilite_keyword : "") << " TAIL" << (format.hilite ? hilite_none : "");
    else if (settings.collapsed_tree.has_value() || settings.keeper_key.has_value())
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << " CURSOR" << (format.hilite ? hilite_none : "");

        if (settings.keeper_key.has_value())
            format.ostr << " " << quoteString(settings.keeper_key.value());

        if (settings.collapsed_tree.has_value())
            format.ostr << " " << cursorToString(settings.collapsed_tree.value());
    }
}

}
