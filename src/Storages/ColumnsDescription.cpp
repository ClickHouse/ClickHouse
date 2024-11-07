#include <Storages/ColumnsDescription.h>

#include <memory>
#include <Compression/CompressionFactory.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/NestedUtils.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/IStorage.h>
#include <Common/Exception.h>
#include <Common/randomSeed.h>
#include <Common/typeid_cast.h>


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

ColumnDescription::ColumnDescription(String name_, DataTypePtr type_, String comment_)
    : name(std::move(name_)), type(std::move(type_)), comment(comment_)
{
}

ColumnDescription::ColumnDescription(String name_, DataTypePtr type_, ASTPtr codec_, String comment_)
    : name(std::move(name_)), type(std::move(type_)), comment(comment_), codec(codec_)
{
}

ColumnDescription & ColumnDescription::operator=(const ColumnDescription & other)
{
    if (this == &other)
        return *this;

    name = other.name;
    type = other.type;
    default_desc = other.default_desc;
    comment = other.comment;
    codec = other.codec ? other.codec->clone() : nullptr;
    settings = other.settings;
    ttl = other.ttl ? other.ttl->clone() : nullptr;
    statistics = other.statistics;

    return *this;
}

ColumnDescription & ColumnDescription::operator=(ColumnDescription && other) noexcept
{
    if (this == &other)
        return *this;

    name = std::move(other.name);
    type = std::move(other.type);
    default_desc = std::move(other.default_desc);
    comment = std::move(other.comment);

    codec = other.codec ? other.codec->clone() : nullptr;
    other.codec.reset();

    settings = std::move(other.settings);

    ttl = other.ttl ? other.ttl->clone() : nullptr;
    other.ttl.reset();

    statistics = std::move(other.statistics);

    return *this;
}

bool ColumnDescription::operator==(const ColumnDescription & other) const
{
    auto ast_to_str = [](const ASTPtr & ast) { return ast ? queryToString(ast) : String{}; };

    return name == other.name
        && type->equals(*other.type)
        && default_desc == other.default_desc
        && statistics == other.statistics
        && ast_to_str(codec) == ast_to_str(other.codec)
        && settings == other.settings
        && ast_to_str(ttl) == ast_to_str(other.ttl);
}

String formatASTStateAware(IAST & ast, IAST::FormatState & state)
{
    WriteBufferFromOwnString buf;
    IAST::FormatSettings settings(buf, true, false);
    ast.formatImpl(settings, state, IAST::FormatStateStacked());
    return buf.str();
}

void ColumnDescription::writeText(WriteBuffer & buf, IAST::FormatState & state, bool include_comment) const
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
        writeEscapedString(formatASTStateAware(*default_desc.expression, state), buf);
    }

    if (!comment.empty() && include_comment)
    {
        writeChar('\t', buf);
        DB::writeText("COMMENT ", buf);
        auto ast = ASTLiteral(Field(comment));
        writeEscapedString(formatASTStateAware(ast, state), buf);
    }

    if (codec)
    {
        writeChar('\t', buf);
        writeEscapedString(formatASTStateAware(*codec, state), buf);
    }

    if (!settings.empty())
    {
        writeChar('\t', buf);
        DB::writeText("SETTINGS ", buf);
        DB::writeText("(", buf);
        ASTSetQuery ast;
        ast.is_standalone = false;
        ast.changes = settings;
        writeEscapedString(formatASTStateAware(ast, state), buf);
        DB::writeText(")", buf);
    }

    if (!statistics.empty())
    {
        writeChar('\t', buf);
        writeEscapedString(formatASTStateAware(*statistics.getAST(), state), buf);
    }

    if (ttl)
    {
        writeChar('\t', buf);
        DB::writeText("TTL ", buf);
        writeEscapedString(formatASTStateAware(*ttl, state), buf);
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
        ASTPtr ast = parseQuery(column_parser, "x T " + modifiers, "column parser", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

        if (auto * col_ast = ast->as<ASTColumnDeclaration>())
        {
            if (col_ast->default_expression)
            {
                default_desc.kind = columnDefaultKindFromString(col_ast->default_specifier);
                default_desc.expression = std::move(col_ast->default_expression);
                default_desc.ephemeral_default = col_ast->ephemeral_default;
            }

            if (col_ast->comment)
                comment = col_ast->comment->as<ASTLiteral &>().value.safeGet<String>();

            if (col_ast->codec)
                codec = CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(col_ast->codec, type, false, true, true);

            if (col_ast->ttl)
                ttl = col_ast->ttl;

            if (col_ast->settings)
                settings = col_ast->settings->as<ASTSetQuery &>().changes;

            if (col_ast->statistics_desc)
                statistics = ColumnStatisticsDescription::fromColumnDeclaration(*col_ast, type);
        }
        else
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Cannot parse column description");
    }
}

ColumnsDescription::ColumnsDescription(std::initializer_list<ColumnDescription> ordinary)
{
    for (auto && elem : ordinary)
        add(elem);
}

ColumnsDescription ColumnsDescription::fromNamesAndTypes(NamesAndTypes ordinary)
{
    ColumnsDescription result;
    for (auto & elem : ordinary)
        result.add(ColumnDescription(std::move(elem.name), std::move(elem.type)));
    return result;
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

    setAliases(std::move(aliases));
}

void ColumnsDescription::setAliases(NamesAndAliases aliases)
{
    for (auto & alias : aliases)
    {
        ColumnDescription description(std::move(alias.name), std::move(alias.type));
        description.default_desc.kind = ColumnDefaultKind::Alias;

        const char * alias_expression_pos = alias.expression.data();
        const char * alias_expression_end = alias_expression_pos + alias.expression.size();
        ParserExpression expression_parser;
        description.default_desc.expression = parseQuery(expression_parser, alias_expression_pos, alias_expression_end, "expression", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

        add(std::move(description));
    }
}


/// We are trying to find first column from end with name `column_name` or with a name beginning with `column_name` and ".".
/// For example "fruits.bananas"
/// names are considered the same if they completely match or `name_without_dot` matches the part of the name to the point
static auto getNameRange(const ColumnsDescription::ColumnsContainer & columns, const String & name_without_dot)
{
    String name_with_dot = name_without_dot + ".";

    /// First we need to check if we have column with name name_without_dot
    /// and if not - check if we have names that start with name_with_dot
    for (auto it = columns.begin(); it != columns.end(); ++it)
    {
        if (it->name == name_without_dot)
            return std::make_pair(it, std::next(it));
    }

    auto begin = std::find_if(columns.begin(), columns.end(), [&](const auto & column){ return startsWith(column.name, name_with_dot); });

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

void ColumnsDescription::add(ColumnDescription column, const String & after_column, bool first, bool add_subcolumns)
{
    if (has(column.name))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                        "Cannot add column {}: column with this name already exists", column.name);

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
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Wrong column name. Cannot find column {} to insert after", after_column);

        insert_it = range.second;
    }

    if (add_subcolumns)
        addSubcolumns(column.name, column.type);
    columns.get<0>().insert(insert_it, std::move(column));
}

void ColumnsDescription::remove(const String & column_name)
{
    auto range = getNameRange(columns, column_name);
    if (range.first == range.second)
    {
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "There is no column {} in table{}", column_name, getHintsMessage(column_name));
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find column {} in ColumnsDescription{}",
                        column_from, getHintsMessage(column_from));
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
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "There is no column {} in table.", column_name);

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
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Wrong column name. Cannot find column {} to insert after", after_column);

        reorder_column([&]() { return getNameRange(columns, after_column).second; });
    }
}

void ColumnsDescription::flattenNested()
{
    for (auto it = columns.begin(); it != columns.end();)
    {
        if (!isNested(it->type))
        {
            ++it;
            continue;
        }

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
        case GetColumnsOptions::None:
        {
            break;
        }
        case GetColumnsOptions::All:
        {
            res = getAll();
            break;
        }
        case GetColumnsOptions::AllPhysicalAndAliases:
        {
            res = getAllPhysical();
            auto aliases = getAliases();
            res.insert(res.end(), aliases.begin(), aliases.end());
            break;
        }
        case GetColumnsOptions::AllPhysical:
        {
            res = getAllPhysical();
            break;
        }
        case GetColumnsOptions::OrdinaryAndAliases:
        {
            res = getOrdinary();
            auto aliases = getAliases();
            res.insert(res.end(), aliases.begin(), aliases.end());
            break;
        }
        case GetColumnsOptions::Ordinary:
        {
            res = getOrdinary();
            break;
        }
        case GetColumnsOptions::Materialized:
        {
            res = getMaterialized();
            break;
        }
        case GetColumnsOptions::Aliases:
        {
            res = getAliases();
            break;
        }
        case GetColumnsOptions::Ephemeral:
        {
            res = getEphemeral();
            break;
        }
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
    if (subcolumns.get<0>().count(column_name))
        return true;

    /// Check for dynamic subcolumns
    auto [ordinary_column_name, dynamic_subcolumn_name] = Nested::splitName(column_name);
    auto it = columns.get<1>().find(ordinary_column_name);
    if (it != columns.get<1>().end() && it->type->hasDynamicSubcolumns())
    {
        if (auto /*dynamic_subcolumn_type*/ _ = it->type->tryGetSubcolumnType(dynamic_subcolumn_name))
            return true;
    }

    return false;
}

const ColumnDescription & ColumnsDescription::get(const String & column_name) const
{
    auto it = columns.get<1>().find(column_name);
    if (it == columns.get<1>().end())
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "There is no column {} in table.", column_name);

    return *it;
}

const ColumnDescription * ColumnsDescription::tryGet(const String & column_name) const
{
    auto it = columns.get<1>().find(column_name);
    return it == columns.get<1>().end() ? nullptr : &(*it);
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

    return GetColumnsOptions::None;
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

        /// Check for dynamic subcolumns.
        auto [ordinary_column_name, dynamic_subcolumn_name] = Nested::splitName(column_name);
        it = columns.get<1>().find(ordinary_column_name);
        if (it != columns.get<1>().end() && it->type->hasDynamicSubcolumns())
        {
            if (auto dynamic_subcolumn_type = it->type->tryGetSubcolumnType(dynamic_subcolumn_name))
                return NameAndTypePair(ordinary_column_name, dynamic_subcolumn_name, it->type, dynamic_subcolumn_type);
        }
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

std::optional<const ColumnDescription> ColumnsDescription::tryGetColumnDescription(const GetColumnsOptions & options, const String & column_name) const
{
    auto it = columns.get<1>().find(column_name);
    if (it != columns.get<1>().end() && (defaultKindToGetKind(it->default_desc.kind) & options.kind))
        return *it;

    if (options.with_subcolumns)
    {
        auto jt = subcolumns.get<0>().find(column_name);
        if (jt != subcolumns.get<0>().end())
            return ColumnDescription{jt->name, jt->type};
    }

    return {};
}

std::optional<const ColumnDescription> ColumnsDescription::tryGetColumnOrSubcolumnDescription(GetColumnsOptions::Kind kind, const String & column_name) const
{
    return tryGetColumnDescription(GetColumnsOptions(kind).withSubcolumns(), column_name);
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

bool ColumnsDescription::hasNotAlias(const String & column_name) const
{
    auto it = columns.get<1>().find(column_name);
    return it != columns.get<1>().end() && it->default_desc.kind != ColumnDefaultKind::Alias;
}

bool ColumnsDescription::hasAlias(const String & column_name) const
{
    auto it = columns.get<1>().find(column_name);
    return it != columns.get<1>().end() && it->default_desc.kind == ColumnDefaultKind::Alias;
}

bool ColumnsDescription::hasColumnOrSubcolumn(GetColumnsOptions::Kind kind, const String & column_name) const
{
    auto it = columns.get<1>().find(column_name);
    if ((it != columns.get<1>().end() && (defaultKindToGetKind(it->default_desc.kind) & kind)) || hasSubcolumn(column_name))
        return true;

    /// Check for dynamic subcolumns.
    auto [ordinary_column_name, dynamic_subcolumn_name] = Nested::splitName(column_name);
    it = columns.get<1>().find(ordinary_column_name);
    if (it != columns.get<1>().end() && it->type->hasDynamicSubcolumns())
    {
        if (auto /*dynamic_subcolumn_type*/ _ = it->type->hasSubcolumn(dynamic_subcolumn_name))
            return true;
    }

    return false;
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

bool ColumnsDescription::hasOnlyOrdinary() const
{
    for (const auto & column : columns)
        if (column.default_desc.kind != ColumnDefaultKind::Default)
            return false;
    return true;
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


String ColumnsDescription::toString(bool include_comments) const
{
    WriteBufferFromOwnString buf;
    IAST::FormatState ast_format_state;

    writeCString("columns format version: 1\n", buf);
    DB::writeText(columns.size(), buf);
    writeCString(" columns:\n", buf);

    for (const ColumnDescription & column : columns)
        column.writeText(buf, ast_format_state, include_comments);

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
    }, ISerialization::SubstreamData(type_in_storage->getDefaultSerialization()).withType(type_in_storage));
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

void getDefaultExpressionInfoInto(const ASTColumnDeclaration & col_decl, const DataTypePtr & data_type, DefaultExpressionsInfo & info)
{
    if (!col_decl.default_expression)
        return;

    /** For columns with explicitly-specified type create two expressions:
    * 1. default_expression aliased as column name with _tmp suffix
    * 2. conversion of expression (1) to explicitly-specified type alias as column name
    */
    if (col_decl.type)
    {
        const auto & final_column_name = col_decl.name;
        const auto tmp_column_name = final_column_name + "_tmp_alter" + toString(randomSeed());
        const auto * data_type_ptr = data_type.get();

        info.expr_list->children.emplace_back(setAlias(
            addTypeConversionToAST(std::make_shared<ASTIdentifier>(tmp_column_name), data_type_ptr->getName()), final_column_name));

        info.expr_list->children.emplace_back(setAlias(col_decl.default_expression->clone(), tmp_column_name));
    }
    else
    {
        info.has_columns_with_default_without_type = true;
        info.expr_list->children.emplace_back(setAlias(col_decl.default_expression->clone(), col_decl.name));
    }
}

namespace
{
std::optional<Block> validateColumnsDefaultsAndGetSampleBlockImpl(ASTPtr default_expr_list, const NamesAndTypesList & all_columns, ContextPtr context, bool get_sample_block)
{
    for (const auto & child : default_expr_list->children)
        if (child->as<ASTSelectQuery>() || child->as<ASTSelectWithUnionQuery>() || child->as<ASTSubquery>())
            throw Exception(ErrorCodes::THERE_IS_NO_DEFAULT_VALUE, "Select query is not allowed in columns DEFAULT expression");

    try
    {
        auto syntax_analyzer_result = TreeRewriter(context).analyze(default_expr_list, all_columns, {}, {}, false, /* allow_self_aliases = */ false);
        const auto actions = ExpressionAnalyzer(default_expr_list, syntax_analyzer_result, context).getActions(true);
        for (const auto & action : actions->getActions())
            if (action.node->type == ActionsDAG::ActionType::ARRAY_JOIN)
                throw Exception(ErrorCodes::THERE_IS_NO_DEFAULT_VALUE, "Unsupported default value that requires ARRAY JOIN action");

        if (!get_sample_block)
            return {};

        return actions->getSampleBlock();
    }
    catch (Exception & ex)
    {
        ex.addMessage("default expression and column type are incompatible.");
        throw;
    }
}
}

void validateColumnsDefaults(ASTPtr default_expr_list, const NamesAndTypesList & all_columns, ContextPtr context)
{
    /// Do not execute the default expressions as they might be heavy, e.g.: access remote servers, etc.
    validateColumnsDefaultsAndGetSampleBlockImpl(default_expr_list, all_columns, context, /*get_sample_block=*/false);
}

Block validateColumnsDefaultsAndGetSampleBlock(ASTPtr default_expr_list, const NamesAndTypesList & all_columns, ContextPtr context)
{
    auto result = validateColumnsDefaultsAndGetSampleBlockImpl(default_expr_list, all_columns, context, /*get_sample_block=*/true);
    chassert(result.has_value());
    return std::move(*result);
}

}
