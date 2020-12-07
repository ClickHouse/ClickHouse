#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/FieldToDataType.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/CommonParsers.h>
#include <Processors/Formats/Impl/ConstantExpressionTemplate.h>
#include <Parsers/ExpressionElementParsers.h>
#include <boost/functional/hash.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SYNTAX_ERROR;
}


struct SpecialParserType
{
    SpecialParserType() = default;
    explicit SpecialParserType(Field::Types::Which main_type_) : main_type(main_type_) {}

    Field::Types::Which main_type = Field::Types::String;
    bool is_nullable = false;
    bool is_array = false;
    bool is_tuple = false;
    /// Type and nullability
    std::vector<std::pair<Field::Types::Which, bool>> nested_types;

    bool useDefaultParser() const
    {
        return main_type == Field::Types::String || (!nested_types.empty()
            && std::all_of(
                nested_types.begin(),
                nested_types.end(),
                [](const auto & type) { return type.first == Field::Types::String; }));
    }
};

struct LiteralInfo
{
    using ASTLiteralPtr = std::shared_ptr<ASTLiteral>;
    LiteralInfo(const ASTLiteralPtr & literal_, const String & column_name_, bool force_nullable_)
            : literal(literal_), dummy_column_name(column_name_), force_nullable(force_nullable_) { }
    ASTLiteralPtr literal;
    String dummy_column_name;
    /// Make column nullable even if expression type is not.
    /// (for literals in functions like ifNull and assumeNotNul, which never return NULL even for NULL arguments)
    bool force_nullable;

    DataTypePtr type;
    SpecialParserType special_parser;
};

static void fillLiteralInfo(DataTypes & nested_types, LiteralInfo & info)
{
    size_t elements_num = nested_types.size();
    info.special_parser.nested_types.reserve(elements_num);

    for (auto & nested_type : nested_types)
    {
        /// It can be Array(Nullable(nested_type)) or Tuple(..., Nullable(nested_type), ...)
        bool is_nullable = false;
        if (const auto * nullable = dynamic_cast<const DataTypeNullable *>(nested_type.get()))
        {
            nested_type = nullable->getNestedType();
            is_nullable = true;
        }

        WhichDataType type_info{nested_type};
        Field::Types::Which field_type;

        /// Promote integers to 64 bit types
        if (type_info.isNativeUInt())
        {
            nested_type = std::make_shared<DataTypeUInt64>();
            field_type = Field::Types::UInt64;
        }
        else if (type_info.isNativeInt())
        {
            nested_type = std::make_shared<DataTypeInt64>();
            field_type = Field::Types::Int64;
        }
        else if (type_info.isFloat64())
        {
            field_type = Field::Types::Float64;
        }
        else if (type_info.isString())
        {
            field_type = Field::Types::String;
        }
        else if (type_info.isArray())
        {
            field_type = Field::Types::Array;
        }
        else if (type_info.isTuple())
        {
            field_type = Field::Types::Tuple;
        }
        else
            throw Exception("Unexpected literal type inside Array: " + nested_type->getName() + ". It's a bug",
                            ErrorCodes::LOGICAL_ERROR);

        if (is_nullable)
            nested_type = std::make_shared<DataTypeNullable>(nested_type);

        info.special_parser.nested_types.emplace_back(field_type, is_nullable);
    }
}

/// Extracts ASTLiterals from expression, replaces them with ASTIdentifiers where needed
/// and deduces data types for dummy columns by field type of literal
class ReplaceLiteralsVisitor
{
public:
    LiteralsInfo replaced_literals;
    const Context & context;

    explicit ReplaceLiteralsVisitor(const Context & context_) : context(context_) { }

    void visit(ASTPtr & ast, bool force_nullable)
    {
        if (visitIfLiteral(ast, force_nullable))
            return;
        if (auto * function = ast->as<ASTFunction>())
            visit(*function, force_nullable);
        else if (ast->as<ASTQueryParameter>())
            return;
        else if (ast->as<ASTIdentifier>())
            throw DB::Exception("Identifier in constant expression", ErrorCodes::SYNTAX_ERROR);
        else
            throw DB::Exception("Syntax error in constant expression", ErrorCodes::SYNTAX_ERROR);
    }

private:
    void visitChildren(ASTPtr & ast, const ColumnNumbers & dont_visit_children, const std::vector<char> & force_nullable)
    {
        for (size_t i = 0; i < ast->children.size(); ++i)
            if (std::find(dont_visit_children.begin(), dont_visit_children.end(), i) == dont_visit_children.end())
                visit(ast->children[i], force_nullable[i]);
    }

    void visit(ASTFunction & function, bool force_nullable)
    {
        if (function.name == "lambda")
            return;

        FunctionOverloadResolverPtr builder = FunctionFactory::instance().get(function.name, context);
        /// Do not replace literals which must be constant
        ColumnNumbers dont_visit_children = builder->getArgumentsThatAreAlwaysConstant();
        /// Allow nullable arguments if function never returns NULL
        ColumnNumbers can_always_be_nullable = builder->getArgumentsThatDontImplyNullableReturnType(function.arguments->children.size());

        std::vector<char> force_nullable_arguments(function.arguments->children.size(), force_nullable);
        for (auto & idx : can_always_be_nullable)
            if (idx < force_nullable_arguments.size())
                force_nullable_arguments[idx] = true;

        visitChildren(function.arguments, dont_visit_children, force_nullable_arguments);
    }

    bool visitIfLiteral(ASTPtr & ast, bool force_nullable)
    {
        auto literal = std::dynamic_pointer_cast<ASTLiteral>(ast);
        if (!literal)
            return false;
        if (literal->begin && literal->end)
        {
            /// Do not replace empty array and array of NULLs
            if (literal->value.getType() == Field::Types::Array)
            {
                const Array & array = literal->value.get<Array>();
                auto not_null = std::find_if_not(array.begin(), array.end(), [](const auto & elem) { return elem.isNull(); });
                if (not_null == array.end())
                    return true;
            }
            String column_name = "_dummy_" + std::to_string(replaced_literals.size());
            replaced_literals.emplace_back(literal, column_name, force_nullable);
            setDataType(replaced_literals.back());
            ast = std::make_shared<ASTIdentifier>(column_name);
        }
        return true;
    }

    static void setDataType(LiteralInfo & info)
    {
        /// Type (Field::Types:Which) of literal in AST can be:
        /// 1. simple literal type: String, UInt64, Int64, Float64, Null
        /// 2. complex literal type: Array or Tuple of simple literals
        /// 3. Array or Tuple of complex literals
        /// Null and empty Array literals are considered as tokens, because template with Nullable(Nothing) or Array(Nothing) is useless.

        Field::Types::Which field_type = info.literal->value.getType();

        /// We have to use ParserNumber instead of type->deserializeAsTextQuoted() for arithmetic types
        /// to check actual type of literal and avoid possible overflow and precision issues.
        info.special_parser = SpecialParserType(field_type);

        /// Do not use 8, 16 and 32 bit types, so template will match all integers
        if (field_type == Field::Types::UInt64)
            info.type = std::make_shared<DataTypeUInt64>();
        else if (field_type == Field::Types::Int64)
            info.type = std::make_shared<DataTypeInt64>();
        else if (field_type == Field::Types::Float64)
            info.type = std::make_shared<DataTypeFloat64>();
        else if (field_type == Field::Types::String)
            info.type = std::make_shared<DataTypeString>();
        else if (field_type == Field::Types::Array)
        {
            info.special_parser.is_array = true;
            info.type = applyVisitor(FieldToDataType(), info.literal->value);
            DataTypes nested_types = { assert_cast<const DataTypeArray &>(*info.type).getNestedType() };
            fillLiteralInfo(nested_types, info);
            info.type = std::make_shared<DataTypeArray>(nested_types[0]);
        }
        else if (field_type == Field::Types::Tuple)
        {
            info.special_parser.is_tuple = true;
            info.type = applyVisitor(FieldToDataType(), info.literal->value);
            auto nested_types = assert_cast<const DataTypeTuple &>(*info.type).getElements();
            fillLiteralInfo(nested_types, info);
            info.type = std::make_shared<DataTypeTuple>(nested_types);
        }
        else
            throw Exception(String("Unexpected literal type ") + info.literal->value.getTypeName() + ". It's a bug",
                            ErrorCodes::LOGICAL_ERROR);

        /// Allow literal to be NULL, if result column has nullable type or if function never returns NULL
        if (info.force_nullable && info.type->canBeInsideNullable())
        {
            info.type = makeNullable(info.type);
            info.special_parser.is_nullable = true;
        }
    }
};


/// Expression template is a sequence of tokens and data types of literals.
/// E.g. template of "position('some string', 'other string') != 0" is
/// ["position", "(", DataTypeString, ",", DataTypeString, ")", "!=", DataTypeUInt64]
ConstantExpressionTemplate::TemplateStructure::TemplateStructure(LiteralsInfo & replaced_literals, TokenIterator expression_begin, TokenIterator expression_end,
                                                                 ASTPtr & expression, const IDataType & result_type, bool null_as_default_, const Context & context)
{
    null_as_default = null_as_default_;

    std::sort(replaced_literals.begin(), replaced_literals.end(), [](const LiteralInfo & a, const LiteralInfo & b)
    {
        return a.literal->begin.value() < b.literal->begin.value();
    });

    /// Make sequence of tokens and determine IDataType by Field::Types:Which for each literal.
    token_after_literal_idx.reserve(replaced_literals.size());
    special_parser.resize(replaced_literals.size());

    TokenIterator prev_end = expression_begin;
    for (size_t i = 0; i < replaced_literals.size(); ++i)
    {
        const LiteralInfo & info = replaced_literals[i];
        if (info.literal->begin.value() < prev_end)
            throw Exception("Cannot replace literals", ErrorCodes::LOGICAL_ERROR);

        while (prev_end < info.literal->begin.value())
        {
            tokens.emplace_back(prev_end->begin, prev_end->size());
            ++prev_end;
        }
        token_after_literal_idx.push_back(tokens.size());

        special_parser[i] = info.special_parser;

        literals.insert({nullptr, info.type, info.dummy_column_name});

        prev_end = info.literal->end.value();
    }

    while (prev_end < expression_end)
    {
        tokens.emplace_back(prev_end->begin, prev_end->size());
        ++prev_end;
    }

    addNodesToCastResult(result_type, expression, null_as_default);

    auto syntax_result = TreeRewriter(context).analyze(expression, literals.getNamesAndTypesList());
    result_column_name = expression->getColumnName();
    actions_on_literals = ExpressionAnalyzer(expression, syntax_result, context).getActions(false);
}

size_t ConstantExpressionTemplate::TemplateStructure::getTemplateHash(const ASTPtr & expression,
                                                                      const LiteralsInfo & replaced_literals,
                                                                      const DataTypePtr & result_column_type,
                                                                      bool null_as_default,
                                                                      const String & salt)
{
    /// TODO distinguish expressions with the same AST and different tokens (e.g. "CAST(expr, 'Type')" and "CAST(expr AS Type)")
    SipHash hash_state;
    hash_state.update(result_column_type->getName());

    expression->updateTreeHash(hash_state);

    for (const auto & info : replaced_literals)
        hash_state.update(info.type->getName());
    hash_state.update(null_as_default);

    /// Allows distinguish expression in the last column in Values format
    hash_state.update(salt);

    IAST::Hash res128;
    hash_state.get128(res128.first, res128.second);
    size_t res = 0;
    boost::hash_combine(res, res128.first);
    boost::hash_combine(res, res128.second);
    return res;
}


ConstantExpressionTemplate::TemplateStructurePtr
ConstantExpressionTemplate::Cache::getFromCacheOrConstruct(const DataTypePtr & result_column_type,
                                                           bool null_as_default,
                                                           TokenIterator expression_begin,
                                                           TokenIterator expression_end,
                                                           const ASTPtr & expression_,
                                                           const Context & context,
                                                           bool * found_in_cache,
                                                           const String & salt)
{
    TemplateStructurePtr res;
    ASTPtr expression = expression_->clone();
    ReplaceLiteralsVisitor visitor(context);
    visitor.visit(expression, result_column_type->isNullable() || null_as_default);
    ReplaceQueryParameterVisitor param_visitor(context.getQueryParameters());
    param_visitor.visit(expression);

    size_t template_hash = TemplateStructure::getTemplateHash(expression, visitor.replaced_literals, result_column_type, null_as_default, salt);
    auto iter = cache.find(template_hash);
    if (iter == cache.end())
    {
        if (max_size <= cache.size())
            cache.clear();
        res = std::make_shared<TemplateStructure>(visitor.replaced_literals, expression_begin, expression_end,
                                                  expression, *result_column_type, null_as_default, context);
        cache.insert({template_hash, res});
        if (found_in_cache)
            *found_in_cache = false;
    }
    else
    {
        /// FIXME process collisions correctly
        res = iter->second;
        if (found_in_cache)
            *found_in_cache = true;
    }

    return res;
}

bool ConstantExpressionTemplate::parseExpression(ReadBuffer & istr, const FormatSettings & format_settings, const Settings & settings)
{
    size_t cur_column = 0;
    try
    {
        if (tryParseExpression(istr, format_settings, cur_column, settings))
        {
            ++rows_count;
            return true;
        }
    }
    catch (DB::Exception & e)
    {
        for (size_t i = 0; i < cur_column; ++i)
            columns[i]->popBack(1);

        if (!isParseError(e.code()))
            throw;

        return false;
    }

    for (size_t i = 0; i < cur_column; ++i)
        columns[i]->popBack(1);
    return false;
}

bool ConstantExpressionTemplate::tryParseExpression(ReadBuffer & istr, const FormatSettings & format_settings, size_t & cur_column, const Settings & settings)
{
    size_t cur_token = 0;
    size_t num_columns = structure->literals.columns();
    while (cur_column < num_columns)
    {
        size_t skip_tokens_until = structure->token_after_literal_idx[cur_column];
        while (cur_token < skip_tokens_until)
        {
            /// TODO skip comments
            skipWhitespaceIfAny(istr);
            if (!checkString(structure->tokens[cur_token++], istr))
                return false;
        }
        skipWhitespaceIfAny(istr);

        const DataTypePtr & type = structure->literals.getByPosition(cur_column).type;
        if (format_settings.values.accurate_types_of_literals && !structure->special_parser[cur_column].useDefaultParser())
        {
            if (!parseLiteralAndAssertType(istr, type.get(), cur_column, settings))
                return false;
        }
        else
            type->deserializeAsTextQuoted(*columns[cur_column], istr, format_settings);

        ++cur_column;
    }
    while (cur_token < structure->tokens.size())
    {
        skipWhitespaceIfAny(istr);
        if (!checkString(structure->tokens[cur_token++], istr))
            return false;
    }

    return true;
}

bool ConstantExpressionTemplate::parseLiteralAndAssertType(ReadBuffer & istr, const IDataType * complex_type, size_t column_idx, const Settings & settings)
{
    using Type = Field::Types::Which;

    /// TODO in case of type mismatch return some hints to deduce new template faster
    if (istr.eof())
        return false;

    SpecialParserType type_info = structure->special_parser[column_idx];

    /// If literal does not fit entirely in the buffer, parsing error will happen.
    /// However, it's possible to deduce new template (or use template from cache) after error like it was template mismatch.

    if (type_info.is_array || type_info.is_tuple)
    {
        /// TODO faster way to check types without using Parsers
        ParserArrayOfLiterals parser_array;
        ParserTupleOfLiterals parser_tuple;

        Tokens tokens_number(istr.position(), istr.buffer().end());
        IParser::Pos iterator(tokens_number, settings.max_parser_depth);
        Expected expected;
        ASTPtr ast;
        if (!parser_array.parse(iterator, ast, expected) && !parser_tuple.parse(iterator, ast, expected))
            return false;

        istr.position() = const_cast<char *>(iterator->begin);

        const Field & collection = ast->as<ASTLiteral &>().value;
        auto collection_type = applyVisitor(FieldToDataType(), collection);

        DataTypes nested_types;
        if (type_info.is_array)
            nested_types = { assert_cast<const DataTypeArray &>(*collection_type).getNestedType() };
        else
            nested_types = assert_cast<const DataTypeTuple &>(*collection_type).getElements();

        for (size_t i = 0; i < nested_types.size(); ++i)
        {
            const auto & [nested_field_type, is_nullable] = type_info.nested_types[i];
            if (is_nullable)
                if (const auto * nullable = dynamic_cast<const DataTypeNullable *>(nested_types[i].get()))
                    nested_types[i] = nullable->getNestedType();

            WhichDataType nested_type_info(nested_types[i]);
            bool are_types_compatible =
                (nested_type_info.isNativeUInt() && nested_field_type == Type::UInt64) ||
                (nested_type_info.isNativeInt()  && nested_field_type == Type::Int64)  ||
                (nested_type_info.isFloat64()    && nested_field_type == Type::Float64);

            if (!are_types_compatible)
                return false;
        }

        Field array_same_types = convertFieldToType(collection, *complex_type, nullptr);
        columns[column_idx]->insert(array_same_types);
        return true;
    }
    else
    {
        Field number;
        if (type_info.is_nullable && 4 <= istr.available() && 0 == strncasecmp(istr.position(), "NULL", 4))
            istr.position() += 4;
        else
        {
            /// ParserNumber::parse(...) is about 20x slower than strtod(...)
            /// because of using ASTPtr, Expected and Tokens, which are not needed here.
            /// Parse numeric literal in the same way, as ParserNumber does, but use strtod and strtoull directly.
            bool negative = *istr.position() == '-';
            if (negative || *istr.position() == '+')
                ++istr.position();

            static constexpr size_t MAX_LENGTH_OF_NUMBER = 319;
            char buf[MAX_LENGTH_OF_NUMBER + 1];
            size_t bytes_to_copy = std::min(istr.available(), MAX_LENGTH_OF_NUMBER);
            memcpy(buf, istr.position(), bytes_to_copy);
            buf[bytes_to_copy] = 0;

            char * pos_double = buf;
            errno = 0;
            Float64 float_value = std::strtod(buf, &pos_double);
            if (pos_double == buf || errno == ERANGE || float_value < 0)
                return false;

            if (negative)
                float_value = -float_value;

            char * pos_integer = buf;
            errno = 0;
            UInt64 uint_value = std::strtoull(buf, &pos_integer, 0);
            if (pos_integer == pos_double && errno != ERANGE && (!negative || uint_value <= (1ULL << 63)))
            {
                istr.position() += pos_integer - buf;
                if (negative && type_info.main_type == Type::Int64)
                    number = static_cast<Int64>(-uint_value);
                else if (!negative && type_info.main_type == Type::UInt64)
                    number = uint_value;
                else
                    return false;
            }
            else if (type_info.main_type == Type::Float64)
            {
                istr.position() += pos_double - buf;
                number = float_value;
            }
            else
                return false;
        }

        columns[column_idx]->insert(number);
        return true;
    }
}

ColumnPtr ConstantExpressionTemplate::evaluateAll(BlockMissingValues & nulls, size_t column_idx, size_t offset)
{
    Block evaluated = structure->literals.cloneWithColumns(std::move(columns));
    columns = structure->literals.cloneEmptyColumns();
    if (!structure->literals.columns())
        evaluated.insert({ColumnConst::create(ColumnUInt8::create(1, 0), rows_count), std::make_shared<DataTypeUInt8>(), "_dummy"});
    structure->actions_on_literals->execute(evaluated);

    if (!evaluated || evaluated.rows() != rows_count)
        throw Exception("Number of rows mismatch after evaluation of batch of constant expressions: got " +
                        std::to_string(evaluated.rows()) + " rows for " + std::to_string(rows_count) + " expressions",
                        ErrorCodes::LOGICAL_ERROR);

    if (!evaluated.has(structure->result_column_name))
        throw Exception("Cannot evaluate template " + structure->result_column_name + ", block structure:\n" + evaluated.dumpStructure(),
                        ErrorCodes::LOGICAL_ERROR);

    rows_count = 0;
    ColumnPtr res = evaluated.getByName(structure->result_column_name).column->convertToFullColumnIfConst();
    if (!structure->null_as_default)
        return res;

    /// Extract column with evaluated expression and mask for NULLs
    const auto & tuple = assert_cast<const ColumnTuple &>(*res);
    if (tuple.tupleSize() != 2)
        throw Exception("Invalid tuple size, it'a a bug", ErrorCodes::LOGICAL_ERROR);
    const auto & is_null = assert_cast<const ColumnUInt8 &>(tuple.getColumn(1));

    for (size_t i = 0; i < is_null.size(); ++i)
        if (is_null.getUInt(i))
            nulls.setBit(column_idx, offset + i);

    return tuple.getColumnPtr(0);
}

void ConstantExpressionTemplate::TemplateStructure::addNodesToCastResult(const IDataType & result_column_type, ASTPtr & expr, bool null_as_default)
{
    /// Replace "expr" with "CAST(expr, 'TypeName')"
    /// or with "(CAST(assumeNotNull(expr as _expression), 'TypeName'), isNull(_expression))" if null_as_default is true
    if (null_as_default)
    {
        expr->setAlias("_expression");
        expr = makeASTFunction("assumeNotNull", std::move(expr));
    }

    expr = makeASTFunction("cast", std::move(expr), std::make_shared<ASTLiteral>(result_column_type.getName()));

    if (null_as_default)
    {
        auto is_null = makeASTFunction("isNull", std::make_shared<ASTIdentifier>("_expression"));
        expr = makeASTFunction("tuple", std::move(expr), std::move(is_null));
    }
}

}
