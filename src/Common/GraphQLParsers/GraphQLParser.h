#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <map>


namespace DB
{

/// Lightweight GraphQL parser for extracting fields and variables.
/// This is not a full GraphQL parser - it focuses on the field extraction
/// use case needed for ATDv2.
///
/// Supports:
/// - Query, Mutation, Subscription operations
/// - Field selection sets
/// - Field arguments
/// - Variables
/// - Fragments (basic)
///
/// Example usage:
///   GraphQLParser parser;
///   if (parser.parse("{ user { name email } }"))
///   {
///       std::string name = parser.extractField("user.name");
///   }
class GraphQLParser
{
public:
    GraphQLParser() = default;

    /// Parse a GraphQL query string.
    /// Returns true on success, false on parse error.
    bool parse(std::string_view query);

    /// Extract a field by dot-separated path.
    /// Returns the field name at the path, or empty string if not found.
    /// Example: extractField("query.user.name") -> "name"
    /// Example: extractField("mutation.createUser.input.email") -> "email"
    std::string extractField(const std::string & path) const;

    /// Extract a variable value by name.
    /// Looks for the variable in the query's variable definitions.
    /// Returns the default value if specified, or the variable type otherwise.
    std::string extractVariable(const std::string & name) const;

    /// Get the operation type (query, mutation, subscription).
    std::string getOperationType() const { return operation_type_; }

    /// Get the operation name (if specified).
    std::string getOperationName() const { return operation_name_; }

    /// Get all field names at a path.
    std::vector<std::string> getFieldsAtPath(const std::string & path) const;

    /// Check if parsing was successful.
    bool isValid() const { return valid_; }

    /// Get the last error message.
    const std::string & getError() const { return error_; }

private:
    /// AST node types
    struct Field
    {
        std::string name;
        std::string alias;
        std::map<std::string, std::string> arguments;
        std::vector<Field> selection_set;
    };

    struct Variable
    {
        std::string name;
        std::string type;
        std::string default_value;
    };

    /// Tokenizer
    struct Token
    {
        enum Type
        {
            END,
            NAME,
            INT,
            FLOAT,
            STRING,
            LBRACE,      // {
            RBRACE,      // }
            LPAREN,      // (
            RPAREN,      // )
            LBRACKET,    // [
            RBRACKET,    // ]
            COLON,       // :
            COMMA,       // ,
            DOLLAR,      // $
            AT,          // @
            BANG,        // !
            EQUALS,      // =
            SPREAD,      // ...
            PIPE,        // |
        };

        Type type = END;
        std::string value;
        size_t pos = 0;
    };

    class Lexer
    {
    public:
        explicit Lexer(std::string_view input) : input_(input), pos_(0) {}
        Token next();
        Token peek();
        size_t position() const { return pos_; }

    private:
        void skipWhitespaceAndComments();
        Token readName();
        Token readNumber();
        Token readString();

        std::string_view input_;
        size_t pos_;
        Token peeked_;
        bool has_peeked_ = false;
    };

    /// Parsing methods
    bool parseDocument();
    bool parseDefinition();
    bool parseOperationDefinition();
    bool parseVariableDefinitions();
    bool parseVariable(Variable & var);
    bool parseSelectionSet(std::vector<Field> & fields);
    bool parseField(Field & field);
    bool parseArguments(std::map<std::string, std::string> & args);
    bool parseValue(std::string & value);

    /// Helper to find field by path
    const Field * findFieldByPath(const std::vector<Field> & fields,
                                  const std::vector<std::string> & path_parts,
                                  size_t depth) const;

    /// Split path by '.'
    static std::vector<std::string> splitPath(const std::string & path);

    /// State
    std::string operation_type_;
    std::string operation_name_;
    std::vector<Field> fields_;
    std::vector<Variable> variables_;
    bool valid_ = false;
    std::string error_;
    std::unique_ptr<Lexer> lexer_;
};

}
