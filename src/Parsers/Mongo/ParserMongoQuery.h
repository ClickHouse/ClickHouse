#pragma once

#include <rapidjson/document.h>

#include <Parsers/IParserBase.h>

#include <Parsers/Mongo/Metadata.h>

namespace DB
{

namespace Mongo
{

/// Base class for Mongo parsers.
/// Contains data, that should be parsed in parseImpl method,
/// metadata about query (for example collection),
/// edge name for parent edge.
///
/// Example:
///
/// { quantity: { $lt: 20 } }
///                 ^
///                 |
///            current vertex
///
/// For this vertex edge_name = "quantity" and data = { [$lt]: 20 }
class IMongoParser
{
protected:
    rapidjson::Value data;
    std::shared_ptr<QueryMetadata> metadata;
    std::string edge_name;

public:
    explicit IMongoParser(rapidjson::Value data_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : data(std::move(data_)), metadata(metadata_), edge_name(edge_name_)
    {
    }

    /// Returns false if data is incorrect and cannot be parsed into node.
    virtual bool parseImpl(ASTPtr & node) = 0;

    virtual ~IMongoParser() = default;
};


/// Creates a parser based on edge name and data.
std::shared_ptr<IMongoParser> createParser(
    rapidjson::Value data_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_, bool literal_as_default = false);

/// Creates a inversed parsed based on edge name and data.
/// Inversed means that mongo syntax have inversed order of elements in tree.
/// Example : $lt operator. In mongo syntax it should have structure like { quantity: { $lt: 20 } }
/// So tree for this json looks like
///
///       quantity
///      /        \
/// (operator <)  20
///
/// But in AST representation looks like
///
///     (operator <)
///    /           \
///  quantity      20
///
/// So some operators needs in special parsers
std::shared_ptr<IMongoParser>
createInversedParser(rapidjson::Value data_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_);


/// Class to connect mongo and clickhouse parsers
class ParserMongoQuery : public IParserBase
{
private:
    // These fields are not used when Mongo is disabled at build time.
    [[maybe_unused]] size_t max_query_size;
    [[maybe_unused]] size_t max_parser_depth;
    [[maybe_unused]] size_t max_parser_backtracks;

    rapidjson::Value data;
    std::shared_ptr<QueryMetadata> metadata;

public:
    explicit ParserMongoQuery(size_t max_query_size_, size_t max_parser_depth_, size_t max_parser_backtracks_)
        : max_query_size(max_query_size_), max_parser_depth(max_parser_depth_), max_parser_backtracks(max_parser_backtracks_)
    {
    }

    void setParsingData(rapidjson::Value data_, std::shared_ptr<QueryMetadata> metadata_)
    {
        data = data_;
        metadata = metadata_;
    }

protected:
    const char * getName() const override { return "Mongo query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

}
