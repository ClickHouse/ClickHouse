#include <Storages/ColumnsDescription.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNested.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Common/typeid_cast.h>
#include <Core/Defines.h>
#include <Compression/CompressionFactory.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/FunctionNameNormalizer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int ILLEGAL_COLUMN;
    extern const int CANNOT_PARSE_TEXT;
    extern const int THERE_IS_NO_DEFAULT_VALUE;
    extern const int LOGICAL_ERROR;
}

ColumnDescription::ColumnDescription(String name_, DataTypePtr type_)
    : name(std::move(name_)), type(std::move(type_))
{
}

bool ColumnDescription::operator==(const ColumnDescription & other) const
{
    auto ast_to_str = [](const ASTPtr & ast) { return ast ? queryToString(ast) : String{}; };

    return name == other.name
        && type->equals(*other.type)
        && default_desc == other.default_desc
        && comment == other.comment
        && ast_to_str(codec) == ast_to_str(other.codec)
        && ast_to_str(ttl) == ast_to_str(other.ttl);
}

void ColumnDescription::writeText(WriteBuffer & buf) const
{
    /// NOTE: Serialization format is insane.

    writeBackQuotedString(name, buf);
    writeChar(' ', buf);
    writeEscapedString(type->getName(), buf);

    if (default_desc.expression)
    {
        writeChar('\t', buf);
        DB::writeText(DB::toString(default_desc.kind), buf);
        writeChar('\t', buf);
        writeEscapedString(queryToString(default_desc.expression), buf);
    }

    if (!comment.empty())
    {
        writeChar('\t', buf);
        DB::writeText("COMMENT ", buf);
        writeEscapedString(queryToString(ASTLiteral(Field(comment))), buf);
    }

    if (codec)
    {
        writeChar('\t', buf);
        writeEscapedString(queryToString(codec), buf);
    }

    if (ttl)
    {
        writeChar('\t', buf);
        DB::writeText("TTL ", buf);
        writeEscapedString(queryToString(ttl), buf);
    }

    writeChar('\n', buf);
}

void ColumnDescription::readText(ReadBuffer & buf)
{
    readBackQuotedString(name, buf);
    assertChar(' ', buf);

    String type_string;
    readEscapedString(type_string, buf);
    type = DataTypeFactory::instance().get(type_string);

    if (checkChar('\t', buf))
    {
        String modifiers;
        readEscapedStringUntilEOL(modifiers, buf);

        ParserColumnDeclaration column_parser(/* require type */ true);
        ASTPtr ast = parseQuery(column_parser, "x T " + modifiers, "column parser", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

        if (auto * col_ast = ast->as<ASTColumnDeclaration>())
        {
            if (col_ast->default_expression)
            {
                default_desc.kind = columnDefaultKindFromString(col_ast->default_specifier);
                default_desc.expression = std::move(col_ast->default_expression);
            }

            if (col_ast->comment)
                comment = col_ast->comment->as<ASTLiteral &>().value.get<String>();

            if (col_ast->codec)
                codec = CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(col_ast->codec, type, false, true);

            if (col_ast->ttl)
                ttl = col_ast->ttl;
        }
        else
            throw Exception("Cannot parse column description", ErrorCodes::CANNOT_PARSE_TEXT);
    }
}


ColumnsDescription::ColumnsDescription(NamesAndTypesList ordinary)
{
    for (auto & elem : ordinary)
        add(ColumnDescription(std::move(elem.name), std::move(elem.type)));
}

ColumnsDescription::ColumnsDescription(NamesAndTypesList ordinary, NamesAndAliases aliases)
{
    for (auto & elem : ordinary)
        add(ColumnDescription(std::move(elem.name), std::move(elem.type)));

    for (auto & alias : aliases)
    {
        ColumnDescription description(std::move(alias.name), std::move(alias.type));
        description.default_desc.kind = ColumnDefaultKind::Alias;

        const char * alias_expression_pos = alias.expression.data();
        const char * alias_expression_end = alias_expression_pos + alias.expression.size();
        ParserExpression expression_parser;
        description.default_desc.expression = parseQuery(expression_parser, alias_expression_pos, alias_expression_end, "expression", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

        add(std::move(description));
    }
}


/// We are trying to find first column from end with name `column_name` or with a name beginning with `column_name` and ".".
/// For example "fruits.bananas"
/// names are considered the same if they completely match or `name_without_dot` matches the part of the name to the point
static auto getNameRange(const ColumnsDescription::ColumnsContainer & columns, const String & name_without_dot)
{
    String name_with_dot = name_without_dot + ".";

    auto begin = columns.begin();
    for (; begin != columns.end(); ++begin)
    {
        if (begin->name == name_without_dot)
            return std::make_pair(begin, std::next(begin));

        if (startsWith(begin->name, name_with_dot))
            break;
    }

    if (begin == columns.end())
        return std::make_pair(begin, begin);

    auto end = std::next(begin);
    for (; end != columns.end(); ++end)
    {
        if (!startsWith(end->name, name_with_dot))
            break;
    }

    return std::make_pair(begin, end);
}

void ColumnsDescription::add(ColumnDescription column, const String & after_column, bool first)
{
    if (has(column.name))
        throw Exception("Cannot add column " + column.name + ": column with this name already exists",
            ErrorCodes::ILLEGAL_COLUMN);

    /// Normalize ASTs to be compatible with InterpreterCreateQuery.
    if (column.default_desc.expression)
        FunctionNameNormalizer::visit(column.default_desc.expression.get());
    if (column.ttl)
        FunctionNameNormalizer::visit(column.ttl.get());

    auto insert_it = columns.cend();

    if (first)
        insert_it = columns.cbegin();
    else if (!after_column.empty())
    {
        auto range = getNameRange(columns, after_column);
        if (range.first == range.second)
            throw Exception("Wrong column name. Cannot find column " + after_column + " to insert after",
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        insert_it = range.second;
    }

    addSubcolumns(column.name, column.type);
    columns.get<0>().insert(insert_it, std::move(column));
}

void ColumnsDescription::remove(const String & column_name)
{
    auto range = getNameRange(columns, column_name);
    if (range.first == range.second)
    {
        String exception_message = fmt::format("There is no column {} in table", column_name);
        appendHintsMessage(exception_message, column_name);
        throw Exception(exception_message, ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    }

    for (auto list_it = range.first; list_it != range.second;)
    {
        removeSubcolumns(list_it->name);
        list_it = columns.get<0>().erase(list_it);
    }
}

void ColumnsDescription::rename(const String & column_from, const String & column_to)
{
    auto it = columns.get<1>().find(column_from);
    if (it == columns.get<1>().end())
    {
        String exception_message = fmt::format("Cannot find column {} in ColumnsDescription", column_from);
        appendHintsMessage(exception_message, column_from);
        throw Exception(exception_message, ErrorCodes::LOGICAL_ERROR);
    }

    columns.get<1>().modify_key(it, [&column_to] (String & old_name)
    {
        old_name = column_to;
    });
}

void ColumnsDescription::modifyColumnOrder(const String & column_name, const String & after_column, bool first)
{
    const auto & reorder_column = [&](auto get_new_pos)
    {
        auto column_range = getNameRange(columns, column_name);

        if (column_range.first == column_range.second)
            throw Exception("There is no column " + column_name + " in table.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        std::vector<ColumnDescription> moving_columns;
        for (auto list_it = column_range.first; list_it != column_range.second;)
        {
            moving_columns.emplace_back(*list_it);
            list_it = columns.get<0>().erase(list_it);
        }

        columns.get<0>().insert(get_new_pos(), moving_columns.begin(), moving_columns.end());
    };

    if (first)
        reorder_column([&]() { return columns.cbegin(); });
    else if (!after_column.empty() && column_name != after_column)
    {
        /// Checked first
        auto range = getNameRange(columns, after_column);
        if (range.first == range.second)
            throw Exception("Wrong column name. Cannot find column " + after_column + " to insert after",
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        reorder_column([&]() { return getNameRange(columns, after_column).second; });
    }
}

void ColumnsDescription::flattenNested()
{
    for (auto it = columns.begin(); it != columns.end();)
    {
        const auto * type_arr = typeid_cast<const DataTypeArray *>(it->type.get());
        if (!type_arr)
        {
            ++it;
            continue;
        }

        const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type_arr->getNestedType().get());
        if (!type_tuple)
        {
            ++it;
            continue;
        }

        if (!type_tuple->haveExplicitNames())
        {
            ++it;
            continue;
        }

        ColumnDescription column = *it;
        removeSubcolumns(column.name);
        it = columns.get<0>().erase(it);

        const DataTypes & elements = type_tuple->getElements();
        const Strings & names = type_tuple->getElementNames();
        size_t tuple_size = elements.size();

        for (size_t i = 0; i < tuple_size; ++i)
        {
            auto nested_column = column;
            /// TODO: what to do with default expressions?
            nested_column.name = Nested::concatenateName(column.name, names[i]);
            nested_column.type = std::make_shared<DataTypeArray>(elements[i]);

            addSubcolumns(nested_column.name, nested_column.type);
            columns.get<0>().insert(it, std::move(nested_column));
        }
    }
}


NamesAndTypesList ColumnsDescription::getOrdinary() const
{
    NamesAndTypesList ret;
    for (const auto & col : columns)
        if (col.default_desc.kind == ColumnDefaultKind::Default)
            ret.emplace_back(col.name, col.type);
    return ret;
}

NamesAndTypesList ColumnsDescription::getInsertable() const
{
    NamesAndTypesList ret;
    for (const auto & col : columns)
        if (col.default_desc.kind == ColumnDefaultKind::Default || col.default_desc.kind == ColumnDefaultKind::Ephemeral)
            ret.emplace_back(col.name, col.type);
    return ret;
}

NamesAndTypesList ColumnsDescription::getMaterialized() const
{
    NamesAndTypesList ret;
    for (const auto & col : columns)
        if (col.default_desc.kind == ColumnDefaultKind::Materialized)
            ret.emplace_back(col.name, col.type);
    return ret;
}

NamesAndTypesList ColumnsDescription::getAliases() const
{
    NamesAndTypesList ret;
    for (const auto & col : columns)
        if (col.default_desc.kind == ColumnDefaultKind::Alias)
            ret.emplace_back(col.name, col.type);
    return ret;
}

NamesAndTypesList ColumnsDescription::getEphemeral() const
{
    NamesAndTypesList ret;
        for (const auto & col : columns)
            if (col.default_desc.kind == ColumnDefaultKind::Ephemeral)
                ret.emplace_back(col.name, col.type);
    return ret;
}

NamesAndTypesList ColumnsDescription::getAll() const
{
    NamesAndTypesList ret;
    for (const auto & col : columns)
        ret.emplace_back(col.name, col.type);
    return ret;
}

NamesAndTypesList ColumnsDescription::getSubcolumns(const String & name_in_storage) const
{
    auto range = subcolumns.get<1>().equal_range(name_in_storage);
    return NamesAndTypesList(range.first, range.second);
}

NamesAndTypesList ColumnsDescription::getNested(const String & column_name) const
{
    auto range = getNameRange(columns, column_name);
    NamesAndTypesList nested;
    for (auto & it = range.first; it != range.second; ++it)
        nested.emplace_back(it->name, it->type);
    return nested;
}

void ColumnsDescription::addSubcolumnsToList(NamesAndTypesList & source_list) const
{
    NamesAndTypesList subcolumns_list;
    for (const auto & col : source_list)
    {
        auto range = subcolumns.get<1>().equal_range(col.name);
        if (range.first != range.second)
            subcolumns_list.insert(subcolumns_list.end(), range.first, range.second);
    }

    source_list.splice(source_list.end(), std::move(subcolumns_list));
}

NamesAndTypesList ColumnsDescription::get(const GetColumnsOptions & options) const
{
    NamesAndTypesList res;
    switch (options.kind)
    {
        case GetColumnsOptions::All:
            res = getAll();
            break;
        case GetColumnsOptions::AllPhysical:
            res = getAllPhysical();
            break;
        case GetColumnsOptions::Ordinary:
            res = getOrdinary();
            break;
        case GetColumnsOptions::Materialized:
            res = getMaterialized();
            break;
        case GetColumnsOptions::Aliases:
            res = getAliases();
            break;
        case GetColumnsOptions::Ephemeral:
            res = getEphemeral();
            break;
    }

    if (options.with_subcolumns)
        addSubcolumnsToList(res);

    return res;
}

bool ColumnsDescription::has(const String & column_name) const
{
    return columns.get<1>().find(column_name) != columns.get<1>().end();
}

bool ColumnsDescription::hasNested(const String & column_name) const
{
    auto range = getNameRange(columns, column_name);
    return range.first != range.second && range.first->name.length() > column_name.length();
}

bool ColumnsDescription::hasSubcolumn(const String & column_name) const
{
    return subcolumns.get<0>().count(column_name);
}

const ColumnDescription & ColumnsDescription::get(const String & column_name) const
{
    auto it = columns.get<1>().find(column_name);
    if (it == columns.get<1>().end())
        throw Exception("There is no column " + column_name + " in table.",
            ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

    return *it;
}

static GetColumnsOptions::Kind defaultKindToGetKind(ColumnDefaultKind kind)
{
    switch (kind)
    {
        case ColumnDefaultKind::Default:
            return GetColumnsOptions::Ordinary;
        case ColumnDefaultKind::Materialized:
            return GetColumnsOptions::Materialized;
        case ColumnDefaultKind::Alias:
            return GetColumnsOptions::Aliases;
        case ColumnDefaultKind::Ephemeral:
            return GetColumnsOptions::Ephemeral;
    }
    __builtin_unreachable();
}

NamesAndTypesList ColumnsDescription::getByNames(const GetColumnsOptions & options, const Names & names) const
{
    NamesAndTypesList res;
    for (const auto & name : names)
    {
        if (auto it = columns.get<1>().find(name); it != columns.get<1>().end())
        {
            auto kind = defaultKindToGetKind(it->default_desc.kind);
            if (options.kind & kind)
            {
                res.emplace_back(name, it->type);
                continue;
            }
        }
        else if (options.with_subcolumns)
        {
            auto jt = subcolumns.get<0>().find(name);
            if (jt != subcolumns.get<0>().end())
            {
                res.push_back(*jt);
                continue;
            }
        }

        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "There is no column {} in table", name);
    }

    return res;
}


NamesAndTypesList ColumnsDescription::getAllPhysical() const
{
    NamesAndTypesList ret;
    for (const auto & col : columns)
        if (col.default_desc.kind != ColumnDefaultKind::Alias && col.default_desc.kind != ColumnDefaultKind::Ephemeral)
            ret.emplace_back(col.name, col.type);
    return ret;
}

Names ColumnsDescription::getNamesOfPhysical() const
{
    Names ret;
    for (const auto & col : columns)
        if (col.default_desc.kind != ColumnDefaultKind::Alias && col.default_desc.kind != ColumnDefaultKind::Ephemeral)
            ret.emplace_back(col.name);
    return ret;
}

std::optional<NameAndTypePair> ColumnsDescription::tryGetColumn(const GetColumnsOptions & options, const String & column_name) const
{
    auto it = columns.get<1>().find(column_name);
    if (it != columns.get<1>().end() && (defaultKindToGetKind(it->default_desc.kind) & options.kind))
        return NameAndTypePair(it->name, it->type);

    if (options.with_subcolumns)
    {
        auto jt = subcolumns.get<0>().find(column_name);
        if (jt != subcolumns.get<0>().end())
            return *jt;
    }

    return {};
}

NameAndTypePair ColumnsDescription::getColumn(const GetColumnsOptions & options, const String & column_name) const
{
    auto column = tryGetColumn(options, column_name);
    if (!column)
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
            "There is no column {} in table.", column_name);

    return *column;
}

std::optional<NameAndTypePair> ColumnsDescription::tryGetColumnOrSubcolumn(GetColumnsOptions::Kind kind, const String & column_name) const
{
    return tryGetColumn(GetColumnsOptions(kind).withSubcolumns(), column_name);
}

NameAndTypePair ColumnsDescription::getColumnOrSubcolumn(GetColumnsOptions::Kind kind, const String & column_name) const
{
    auto column = tryGetColumnOrSubcolumn(kind, column_name);
    if (!column)
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
            "There is no column or subcolumn {} in table.", column_name);

    return *column;
}

std::optional<NameAndTypePair> ColumnsDescription::tryGetPhysical(const String & column_name) const
{
    return tryGetColumn(GetColumnsOptions::AllPhysical, column_name);
}

NameAndTypePair ColumnsDescription::getPhysical(const String & column_name) const
{
    auto column = tryGetPhysical(column_name);
    if (!column)
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
            "There is no physical column {} in table.", column_name);

    return *column;
}

bool ColumnsDescription::hasPhysical(const String & column_name) const
{
    auto it = columns.get<1>().find(column_name);
    return it != columns.get<1>().end() &&
        it->default_desc.kind != ColumnDefaultKind::Alias && it->default_desc.kind != ColumnDefaultKind::Ephemeral;
}

bool ColumnsDescription::hasColumnOrSubcolumn(GetColumnsOptions::Kind kind, const String & column_name) const
{
    auto it = columns.get<1>().find(column_name);
    return (it != columns.get<1>().end()
        && (defaultKindToGetKind(it->default_desc.kind) & kind))
            || hasSubcolumn(column_name);
}

bool ColumnsDescription::hasColumnOrNested(GetColumnsOptions::Kind kind, const String & column_name) const
{
    auto range = getNameRange(columns, column_name);
    return range.first != range.second &&
        defaultKindToGetKind(range.first->default_desc.kind) & kind;
}

bool ColumnsDescription::hasDefaults() const
{
    for (const auto & column : columns)
        if (column.default_desc.expression)
            return true;
    return false;
}

ColumnDefaults ColumnsDescription::getDefaults() const
{
    ColumnDefaults ret;
    for (const auto & column : columns)
        if (column.default_desc.expression)
            ret.emplace(column.name, column.default_desc);

    return ret;
}

bool ColumnsDescription::hasDefault(const String & column_name) const
{
    auto it = columns.get<1>().find(column_name);
    return it != columns.get<1>().end() && it->default_desc.expression;
}

std::optional<ColumnDefault> ColumnsDescription::getDefault(const String & column_name) const
{
    auto it = columns.get<1>().find(column_name);
    if (it != columns.get<1>().end() && it->default_desc.expression)
        return it->default_desc;

    return {};
}


bool ColumnsDescription::hasCompressionCodec(const String & column_name) const
{
    const auto it = columns.get<1>().find(column_name);

    return it != columns.get<1>().end() && it->codec != nullptr;
}

CompressionCodecPtr ColumnsDescription::getCodecOrDefault(const String & column_name, CompressionCodecPtr default_codec) const
{
    const auto it = columns.get<1>().find(column_name);

    if (it == columns.get<1>().end() || !it->codec)
        return default_codec;

    return CompressionCodecFactory::instance().get(it->codec, it->type, default_codec);
}

CompressionCodecPtr ColumnsDescription::getCodecOrDefault(const String & column_name) const
{
    return getCodecOrDefault(column_name, CompressionCodecFactory::instance().getDefaultCodec());
}

ASTPtr ColumnsDescription::getCodecDescOrDefault(const String & column_name, CompressionCodecPtr default_codec) const
{
    const auto it = columns.get<1>().find(column_name);

    if (it == columns.get<1>().end() || !it->codec)
        return default_codec->getFullCodecDesc();

    return it->codec;
}

ColumnsDescription::ColumnTTLs ColumnsDescription::getColumnTTLs() const
{
    ColumnTTLs ret;
    for (const auto & column : columns)
        if (column.ttl)
            ret.emplace(column.name, column.ttl);
    return ret;
}

void ColumnsDescription::resetColumnTTLs()
{
    std::vector<ColumnDescription> old_columns;
    old_columns.reserve(columns.size());
    for (const auto & col : columns)
        old_columns.emplace_back(col);

    columns.clear();

    for (auto & col : old_columns)
    {
        col.ttl.reset();
        add(col);
    }
}


String ColumnsDescription::toString() const
{
    WriteBufferFromOwnString buf;

    writeCString("columns format version: 1\n", buf);
    DB::writeText(columns.size(), buf);
    writeCString(" columns:\n", buf);

    for (const ColumnDescription & column : columns)
        column.writeText(buf);

    return buf.str();
}

ColumnsDescription ColumnsDescription::parse(const String & str)
{
    ReadBufferFromString buf{str};

    assertString("columns format version: 1\n", buf);
    size_t count{};
    readText(count, buf);
    assertString(" columns:\n", buf);

    ColumnsDescription result;
    for (size_t i = 0; i < count; ++i)
    {
        ColumnDescription column;
        column.readText(buf);
        buf.ignore(1); /// ignore new line
        result.add(column);
    }

    assertEOF(buf);
    return result;
}

void ColumnsDescription::addSubcolumns(const String & name_in_storage, const DataTypePtr & type_in_storage)
{
    IDataType::forEachSubcolumn([&](const auto &, const auto & subname, const auto & subdata)
    {
        auto subcolumn = NameAndTypePair(name_in_storage, subname, type_in_storage, subdata.type);

        if (has(subcolumn.name))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Cannot add subcolumn {}: column with this name already exists", subcolumn.name);

        subcolumns.get<0>().insert(std::move(subcolumn));
    }, {type_in_storage->getDefaultSerialization(), type_in_storage, nullptr, nullptr});
}

void ColumnsDescription::removeSubcolumns(const String & name_in_storage)
{
    auto range = subcolumns.get<1>().equal_range(name_in_storage);
    if (range.first != range.second)
        subcolumns.get<1>().erase(range.first, range.second);
}

std::vector<String> ColumnsDescription::getAllRegisteredNames() const
{
    std::vector<String> names;
    names.reserve(columns.size());
    for (const auto & column : columns)
    {
        if (column.name.find('.') == std::string::npos)
            names.push_back(column.name);
    }
    return names;
}

Block validateColumnsDefaultsAndGetSampleBlock(ASTPtr default_expr_list, const NamesAndTypesList & all_columns, ContextPtr context)
{
    for (const auto & child : default_expr_list->children)
        if (child->as<ASTSelectQuery>() || child->as<ASTSelectWithUnionQuery>() || child->as<ASTSubquery>())
            throw Exception("Select query is not allowed in columns DEFAULT expression", ErrorCodes::THERE_IS_NO_DEFAULT_VALUE);

    try
    {
        auto syntax_analyzer_result = TreeRewriter(context).analyze(default_expr_list, all_columns, {}, {}, false, /* allow_self_aliases = */ false);
        const auto actions = ExpressionAnalyzer(default_expr_list, syntax_analyzer_result, context).getActions(true);
        for (const auto & action : actions->getActions())
            if (action.node->type == ActionsDAG::ActionType::ARRAY_JOIN)
                throw Exception("Unsupported default value that requires ARRAY JOIN action", ErrorCodes::THERE_IS_NO_DEFAULT_VALUE);

        return actions->getSampleBlock();
    }
    catch (Exception & ex)
    {
        ex.addMessage("default expression and column type are incompatible.");
        throw;
    }
}

}
