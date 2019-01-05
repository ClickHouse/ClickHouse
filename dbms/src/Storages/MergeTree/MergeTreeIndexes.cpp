#include <Storages/MergeTree/MergeTreeIndexes.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int UNKNOWN_EXCEPTION;
}


void MergeTreeIndexes::writeText(DB::WriteBuffer &ostr) const
{
    writeString("indexes format version: 1\n", ostr);
    DB::writeText(size(), ostr);
    writeString(" indexes:\n", ostr);
    for (auto index : *this) {
        index->writeText(ostr);
        writeChar('\n', ostr);
    }
}


void MergeTreeIndexes::readText(DB::ReadBuffer &istr)
{
    const MergeTreeIndexFactory & factory = MergeTreeIndexFactory::instance();

    assertString("indexes format version: 1\n", istr);
    size_t count;
    DB::readText(count, istr);
    assertString(" indexes:\n", istr);
    reserve(count);
    for (size_t i = 0; i < count; ++i) {
        String index_descr;
        readString(index_descr, istr);
        emplace_back(factory.get(index_descr));
        assertChar('\n', istr);
    }
}


void MergeTreeIndexFactory::registerIndex(const std::string &name, Creator creator)
{
    if (!indexes.emplace(name, std::move(creator)).second)
        throw Exception("MergeTreeIndexFactory: the Index creator name '" + name + "' is not unique",
                        ErrorCodes::LOGICAL_ERROR);
}

std::unique_ptr<MergeTreeIndex> MergeTreeIndexFactory::get(std::shared_ptr<ASTIndexDeclaration> node) const
{
    if (!node->type)
        throw Exception(
                "for INDEX TYPE is required",
                ErrorCodes::INCORRECT_QUERY);
    auto it = indexes.find(node->type->name);
    if (it == indexes.end())
        throw Exception(
                "Unknown Index type '" + node->type->name + "'",
                ErrorCodes::INCORRECT_QUERY);
    return it->second(node);
}

std::unique_ptr<MergeTreeIndex> MergeTreeIndexFactory::get(const String & description) const
{
    ParserIndexDeclaration parser;
    ASTPtr ast = parseQuery(parser, description.data(), description.data() + description.size(), "index factory", 0);
    return get(std::dynamic_pointer_cast<ASTIndexDeclaration>(ast));
}

}