#include <Common/GraphQLParsers/GraphQLParser.h>

#include <cctype>
#include <sstream>


namespace DB
{

void GraphQLParser::Lexer::skipWhitespaceAndComments()
{
    while (pos_ < input_.size())
    {
        char c = input_[pos_];

        /// Skip whitespace
        if (std::isspace(static_cast<unsigned char>(c)))
        {
            ++pos_;
            continue;
        }

        /// Skip comments
        if (c == '#')
        {
            while (pos_ < input_.size() && input_[pos_] != '\n')
                ++pos_;
            continue;
        }

        /// Skip commas (they're insignificant in GraphQL)
        if (c == ',')
        {
            ++pos_;
            continue;
        }

        break;
    }
}

GraphQLParser::Token GraphQLParser::Lexer::readName()
{
    Token token;
    token.type = Token::NAME;
    token.pos = pos_;

    while (pos_ < input_.size())
    {
        char c = input_[pos_];
        if (std::isalnum(static_cast<unsigned char>(c)) || c == '_')
        {
            token.value += c;
            ++pos_;
        }
        else
            break;
    }

    return token;
}

GraphQLParser::Token GraphQLParser::Lexer::readNumber()
{
    Token token;
    token.type = Token::INT;
    token.pos = pos_;

    if (pos_ < input_.size() && input_[pos_] == '-')
    {
        token.value += '-';
        ++pos_;
    }

    while (pos_ < input_.size() && std::isdigit(static_cast<unsigned char>(input_[pos_])))
    {
        token.value += input_[pos_];
        ++pos_;
    }

    if (pos_ < input_.size() && input_[pos_] == '.')
    {
        token.type = Token::FLOAT;
        token.value += '.';
        ++pos_;

        while (pos_ < input_.size() && std::isdigit(static_cast<unsigned char>(input_[pos_])))
        {
            token.value += input_[pos_];
            ++pos_;
        }
    }

    /// Handle exponent
    if (pos_ < input_.size() && (input_[pos_] == 'e' || input_[pos_] == 'E'))
    {
        token.type = Token::FLOAT;
        token.value += input_[pos_];
        ++pos_;

        if (pos_ < input_.size() && (input_[pos_] == '+' || input_[pos_] == '-'))
        {
            token.value += input_[pos_];
            ++pos_;
        }

        while (pos_ < input_.size() && std::isdigit(static_cast<unsigned char>(input_[pos_])))
        {
            token.value += input_[pos_];
            ++pos_;
        }
    }

    return token;
}

GraphQLParser::Token GraphQLParser::Lexer::readString()
{
    Token token;
    token.type = Token::STRING;
    token.pos = pos_;

    char quote = input_[pos_];
    ++pos_;

    /// Check for block string (""")
    bool block_string = false;
    if (pos_ + 1 < input_.size() && input_[pos_] == quote && input_[pos_ + 1] == quote)
    {
        block_string = true;
        pos_ += 2;
    }

    while (pos_ < input_.size())
    {
        char c = input_[pos_];

        if (block_string)
        {
            if (c == quote && pos_ + 2 < input_.size()
                && input_[pos_ + 1] == quote && input_[pos_ + 2] == quote)
            {
                pos_ += 3;
                break;
            }
            token.value += c;
            ++pos_;
        }
        else
        {
            if (c == quote)
            {
                ++pos_;
                break;
            }
            if (c == '\\' && pos_ + 1 < input_.size())
            {
                ++pos_;
                char escaped = input_[pos_];
                switch (escaped)
                {
                    case 'n': token.value += '\n'; break;
                    case 'r': token.value += '\r'; break;
                    case 't': token.value += '\t'; break;
                    case '\\': token.value += '\\'; break;
                    case '"': token.value += '"'; break;
                    default: token.value += escaped; break;
                }
                ++pos_;
            }
            else
            {
                token.value += c;
                ++pos_;
            }
        }
    }

    return token;
}

GraphQLParser::Token GraphQLParser::Lexer::next()
{
    if (has_peeked_)
    {
        has_peeked_ = false;
        return peeked_;
    }

    skipWhitespaceAndComments();

    Token token;
    token.pos = pos_;

    if (pos_ >= input_.size())
    {
        token.type = Token::END;
        return token;
    }

    char c = input_[pos_];

    /// Check for spread operator
    if (c == '.' && pos_ + 2 < input_.size()
        && input_[pos_ + 1] == '.' && input_[pos_ + 2] == '.')
    {
        token.type = Token::SPREAD;
        token.value = "...";
        pos_ += 3;
        return token;
    }

    /// Single character tokens
    switch (c)
    {
        case '{': token.type = Token::LBRACE; token.value = "{"; ++pos_; return token;
        case '}': token.type = Token::RBRACE; token.value = "}"; ++pos_; return token;
        case '(': token.type = Token::LPAREN; token.value = "("; ++pos_; return token;
        case ')': token.type = Token::RPAREN; token.value = ")"; ++pos_; return token;
        case '[': token.type = Token::LBRACKET; token.value = "["; ++pos_; return token;
        case ']': token.type = Token::RBRACKET; token.value = "]"; ++pos_; return token;
        case ':': token.type = Token::COLON; token.value = ":"; ++pos_; return token;
        case '$': token.type = Token::DOLLAR; token.value = "$"; ++pos_; return token;
        case '@': token.type = Token::AT; token.value = "@"; ++pos_; return token;
        case '!': token.type = Token::BANG; token.value = "!"; ++pos_; return token;
        case '=': token.type = Token::EQUALS; token.value = "="; ++pos_; return token;
        case '|': token.type = Token::PIPE; token.value = "|"; ++pos_; return token;
        default: break;
    }

    /// String
    if (c == '"')
        return readString();

    /// Number
    if (c == '-' || std::isdigit(static_cast<unsigned char>(c)))
        return readNumber();

    /// Name/Keyword
    if (std::isalpha(static_cast<unsigned char>(c)) || c == '_')
        return readName();

    /// Unknown character - skip it
    ++pos_;
    return next();
}

GraphQLParser::Token GraphQLParser::Lexer::peek()
{
    if (!has_peeked_)
    {
        peeked_ = next();
        has_peeked_ = true;
    }
    return peeked_;
}


bool GraphQLParser::parse(std::string_view query)
{
    valid_ = false;
    error_.clear();
    operation_type_.clear();
    operation_name_.clear();
    fields_.clear();
    variables_.clear();

    lexer_ = std::make_unique<Lexer>(query);

    try
    {
        valid_ = parseDocument();
    }
    catch (const std::exception & e)
    {
        error_ = e.what();
        valid_ = false;
    }

    return valid_;
}

bool GraphQLParser::parseDocument()
{
    while (lexer_->peek().type != Token::END)
    {
        if (!parseDefinition())
            return false;
    }
    return true;
}

bool GraphQLParser::parseDefinition()
{
    Token token = lexer_->peek();

    if (token.type == Token::LBRACE)
    {
        /// Anonymous query
        operation_type_ = "query";
        return parseSelectionSet(fields_);
    }

    if (token.type == Token::NAME)
    {
        if (token.value == "query" || token.value == "mutation" || token.value == "subscription")
            return parseOperationDefinition();

        if (token.value == "fragment")
        {
            /// Skip fragment definitions for now
            while (lexer_->peek().type != Token::END)
            {
                token = lexer_->next();
                if (token.type == Token::RBRACE)
                    break;
            }
            return true;
        }
    }

    error_ = "Unexpected token: " + token.value;
    return false;
}

bool GraphQLParser::parseOperationDefinition()
{
    Token token = lexer_->next();
    operation_type_ = token.value;

    token = lexer_->peek();

    /// Optional operation name
    if (token.type == Token::NAME)
    {
        operation_name_ = token.value;
        lexer_->next();
        token = lexer_->peek();
    }

    /// Optional variable definitions
    if (token.type == Token::LPAREN)
    {
        if (!parseVariableDefinitions())
            return false;
    }

    /// Selection set is required
    return parseSelectionSet(fields_);
}

bool GraphQLParser::parseVariableDefinitions()
{
    Token token = lexer_->next();  /// Consume (
    if (token.type != Token::LPAREN)
    {
        error_ = "Expected (";
        return false;
    }

    while (lexer_->peek().type != Token::RPAREN)
    {
        Variable var;
        if (!parseVariable(var))
            return false;
        variables_.push_back(var);
    }

    lexer_->next();  /// Consume )
    return true;
}

bool GraphQLParser::parseVariable(Variable & var)
{
    Token token = lexer_->next();
    if (token.type != Token::DOLLAR)
    {
        error_ = "Expected $";
        return false;
    }

    token = lexer_->next();
    if (token.type != Token::NAME)
    {
        error_ = "Expected variable name";
        return false;
    }
    var.name = token.value;

    token = lexer_->next();
    if (token.type != Token::COLON)
    {
        error_ = "Expected :";
        return false;
    }

    /// Parse type (simplified - just collect tokens until = or ) or $)
    while (true)
    {
        token = lexer_->peek();
        if (token.type == Token::EQUALS || token.type == Token::RPAREN
            || token.type == Token::DOLLAR || token.type == Token::END)
            break;

        token = lexer_->next();
        var.type += token.value;
    }

    /// Optional default value
    if (lexer_->peek().type == Token::EQUALS)
    {
        lexer_->next();  /// Consume =
        if (!parseValue(var.default_value))
            return false;
    }

    return true;
}

bool GraphQLParser::parseSelectionSet(std::vector<Field> & fields)
{
    Token token = lexer_->next();
    if (token.type != Token::LBRACE)
    {
        error_ = "Expected {";
        return false;
    }

    while (lexer_->peek().type != Token::RBRACE)
    {
        if (lexer_->peek().type == Token::END)
        {
            error_ = "Unexpected end of input";
            return false;
        }

        /// Handle spread operator
        if (lexer_->peek().type == Token::SPREAD)
        {
            lexer_->next();  /// Consume ...
            token = lexer_->next();  /// Fragment name or "on"
            /// Skip fragment spread for now
            if (token.value == "on")
            {
                lexer_->next();  /// Type name
                if (lexer_->peek().type == Token::LBRACE)
                {
                    std::vector<Field> inline_fields;
                    if (!parseSelectionSet(inline_fields))
                        return false;
                    fields.insert(fields.end(), inline_fields.begin(), inline_fields.end());
                }
            }
            continue;
        }

        Field field;
        if (!parseField(field))
            return false;
        fields.push_back(field);
    }

    lexer_->next();  /// Consume }
    return true;
}

bool GraphQLParser::parseField(Field & field)
{
    Token token = lexer_->next();
    if (token.type != Token::NAME)
    {
        error_ = "Expected field name";
        return false;
    }

    field.name = token.value;

    /// Check for alias
    if (lexer_->peek().type == Token::COLON)
    {
        lexer_->next();  /// Consume :
        field.alias = field.name;
        token = lexer_->next();
        if (token.type != Token::NAME)
        {
            error_ = "Expected field name after alias";
            return false;
        }
        field.name = token.value;
    }

    /// Optional arguments
    if (lexer_->peek().type == Token::LPAREN)
    {
        if (!parseArguments(field.arguments))
            return false;
    }

    /// Skip directives
    while (lexer_->peek().type == Token::AT)
    {
        lexer_->next();  /// Consume @
        lexer_->next();  /// Directive name
        if (lexer_->peek().type == Token::LPAREN)
        {
            std::map<std::string, std::string> dummy;
            if (!parseArguments(dummy))
                return false;
        }
    }

    /// Optional selection set
    if (lexer_->peek().type == Token::LBRACE)
    {
        if (!parseSelectionSet(field.selection_set))
            return false;
    }

    return true;
}

bool GraphQLParser::parseArguments(std::map<std::string, std::string> & args)
{
    Token token = lexer_->next();  /// Consume (
    if (token.type != Token::LPAREN)
    {
        error_ = "Expected (";
        return false;
    }

    while (lexer_->peek().type != Token::RPAREN)
    {
        if (lexer_->peek().type == Token::END)
        {
            error_ = "Unexpected end of input in arguments";
            return false;
        }

        token = lexer_->next();
        if (token.type != Token::NAME)
        {
            error_ = "Expected argument name";
            return false;
        }
        std::string arg_name = token.value;

        token = lexer_->next();
        if (token.type != Token::COLON)
        {
            error_ = "Expected :";
            return false;
        }

        std::string arg_value;
        if (!parseValue(arg_value))
            return false;

        args[arg_name] = arg_value;
    }

    lexer_->next();  /// Consume )
    return true;
}

bool GraphQLParser::parseValue(std::string & value)
{
    Token token = lexer_->peek();

    switch (token.type)
    {
        case Token::INT:
        case Token::FLOAT:
        case Token::STRING:
        case Token::NAME:  /// Enum values, true, false, null
            value = lexer_->next().value;
            return true;

        case Token::DOLLAR:
        {
            lexer_->next();  /// Consume $
            token = lexer_->next();
            value = "$" + token.value;
            return true;
        }

        case Token::LBRACKET:
        {
            /// List value
            value = "[";
            lexer_->next();
            bool first = true;
            while (lexer_->peek().type != Token::RBRACKET)
            {
                if (!first) value += ", ";
                first = false;
                std::string item;
                if (!parseValue(item))
                    return false;
                value += item;
            }
            lexer_->next();  /// Consume ]
            value += "]";
            return true;
        }

        case Token::LBRACE:
        {
            /// Object value
            value = "{";
            lexer_->next();
            bool first = true;
            while (lexer_->peek().type != Token::RBRACE)
            {
                if (!first) value += ", ";
                first = false;

                token = lexer_->next();
                value += token.value + ": ";

                lexer_->next();  /// Consume :

                std::string field_value;
                if (!parseValue(field_value))
                    return false;
                value += field_value;
            }
            lexer_->next();  /// Consume }
            value += "}";
            return true;
        }

        default:
            error_ = "Unexpected token in value";
            return false;
    }
}

std::vector<std::string> GraphQLParser::splitPath(const std::string & path)
{
    std::vector<std::string> parts;
    std::string current;

    for (char c : path)
    {
        if (c == '.')
        {
            if (!current.empty())
            {
                parts.push_back(current);
                current.clear();
            }
        }
        else
        {
            current += c;
        }
    }

    if (!current.empty())
        parts.push_back(current);

    return parts;
}

const GraphQLParser::Field * GraphQLParser::findFieldByPath(
    const std::vector<Field> & fields,
    const std::vector<std::string> & path_parts,
    size_t depth) const
{
    if (depth >= path_parts.size())
        return nullptr;

    const std::string & target = path_parts[depth];

    for (const auto & field : fields)
    {
        /// Match by name or alias
        if (field.name == target || field.alias == target)
        {
            if (depth == path_parts.size() - 1)
                return &field;

            /// Continue searching in selection set
            if (!field.selection_set.empty())
                return findFieldByPath(field.selection_set, path_parts, depth + 1);
        }
    }

    return nullptr;
}

std::string GraphQLParser::extractField(const std::string & path) const
{
    if (!valid_)
        return {};

    auto parts = splitPath(path);
    if (parts.empty())
        return {};

    /// Handle operation type prefix
    size_t start_depth = 0;
    if (!parts.empty() && (parts[0] == "query" || parts[0] == "mutation" || parts[0] == "subscription"))
    {
        if (parts[0] != operation_type_)
            return {};
        start_depth = 1;
    }

    if (start_depth >= parts.size())
        return {};

    const Field * field = findFieldByPath(fields_, parts, start_depth);
    if (field)
        return field->name;

    return {};
}

std::string GraphQLParser::extractVariable(const std::string & name) const
{
    if (!valid_)
        return {};

    for (const auto & var : variables_)
    {
        if (var.name == name)
        {
            if (!var.default_value.empty())
                return var.default_value;
            return var.type;
        }
    }

    return {};
}

std::vector<std::string> GraphQLParser::getFieldsAtPath(const std::string & path) const
{
    std::vector<std::string> result;
    if (!valid_)
        return result;

    auto parts = splitPath(path);

    /// Handle operation type prefix
    size_t start_depth = 0;
    if (!parts.empty() && (parts[0] == "query" || parts[0] == "mutation" || parts[0] == "subscription"))
    {
        if (parts[0] != operation_type_)
            return result;
        start_depth = 1;
    }

    const std::vector<Field> * current_fields = &fields_;

    for (size_t i = start_depth; i < parts.size(); ++i)
    {
        const std::string & target = parts[i];
        bool found = false;

        for (const auto & field : *current_fields)
        {
            if (field.name == target || field.alias == target)
            {
                current_fields = &field.selection_set;
                found = true;
                break;
            }
        }

        if (!found)
            return result;
    }

    for (const auto & field : *current_fields)
        result.push_back(field.name);

    return result;
}

}
