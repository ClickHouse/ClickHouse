#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/RenameColumnVisitor.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/ColumnsDescription.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Whether `name` or one of its dotted prefixes longer than `min_length` is a column of the table.
bool hasColumnPrefix(const ColumnsDescription & columns, const String & name, size_t min_length)
{
    for (size_t pos = name.find('.', min_length + 1); pos != String::npos; pos = name.find('.', pos + 1))
    {
        if (columns.has(name.substr(0, pos)))
            return true;
    }
    return columns.has(name);
}

}

void RenameColumnData::visit(ASTIdentifier & identifier, ASTPtr & ast) const
{
    // TODO(ilezhankin): make proper rename
    std::optional<String> identifier_column_name = IdentifierSemantic::getColumnName(identifier);
    if (!identifier_column_name)
        return;

    if (identifier_column_name == column_name)
    {
        identifier.setShortName(rename_to);
    }
    else if (identifier_column_name->starts_with(column_name + "."))
    {
        /// `column_name.x` (written as a compound identifier, or as one part as the key expressions built from the metadata
        /// of a data lake are written) is renamed if it refers to a subcolumn of `column_name`, not to another column such
        /// as a physical `column_name.x` in `MergeTree` with `share_nested_offsets = 0`, or to a subcolumn of one.
        if (columns_before)
        {
            if (auto column = columns_before->tryGetColumnOrSubcolumn(GetColumnsOptions::All, *identifier_column_name))
            {
                if (column->getNameInStorage() != column_name)
                    return;
            }
            else if (hasColumnPrefix(*columns_before, *identifier_column_name, column_name.size()))
                return;
        }

        if (columns_after)
        {
            String renamed_name = rename_to + identifier_column_name->substr(column_name.size());
            if (hasColumnPrefix(*columns_after, renamed_name, rename_to.size()))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Cannot rename column {} to {}: the reference {} would refer to the column {}",
                    backQuote(column_name), backQuote(rename_to), backQuote(*identifier_column_name), backQuote(renamed_name));
        }

        if (identifier.compound())
        {
            /// The column name can span several parts, as the `n.x` of a `Nested` does, or be a part of one, as in `s.x`.z.
            size_t prefix_parts = 0;
            String prefix;
            while (prefix != column_name && !prefix.starts_with(column_name + ".") && prefix_parts < identifier.name_parts.size())
            {
                if (prefix_parts)
                    prefix += '.';
                prefix += identifier.name_parts[prefix_parts];
                ++prefix_parts;
            }
            Strings name_parts;
            if (prefix == column_name)
                boost::split(name_parts, rename_to, boost::is_any_of("."));
            else if (prefix.starts_with(column_name + "."))
                name_parts.push_back(rename_to + prefix.substr(column_name.size()));
            else
                return;
            name_parts.insert(name_parts.end(), identifier.name_parts.begin() + prefix_parts, identifier.name_parts.end());
            auto renamed = make_intrusive<ASTIdentifier>(std::move(name_parts));
            renamed->setAlias(identifier.tryGetAlias());
            ast = std::move(renamed);
        }
        else
        {
            identifier.setShortName(rename_to + identifier_column_name->substr(column_name.size()));
        }
    }
}

}
