#pragma once

#include <Common/assert_cast.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Parsers/ASTTableOverrides.h>

namespace DB
{

struct StorageInMemoryMetadata;

using NameToTypeMap = std::map<String, DataTypePtr>;

struct TableOverrideAnalyzer
{
    struct Result
    {
        NameToTypeMap existing_types;
        NamesAndTypes order_by_columns;
        NamesAndTypes primary_key_columns;
        NamesAndTypes partition_by_columns;
        NamesAndTypes sample_by_columns;
        NamesAndTypes ttl_columns;
        NamesAndTypes added_columns;
        NamesAndTypes modified_columns;

        void appendTo(WriteBuffer &);
    };

    ASTTableOverride * override;

    explicit TableOverrideAnalyzer(ASTPtr ast) : override(assert_cast<ASTTableOverride *>(ast.get())) { }

    void analyze(const StorageInMemoryMetadata & metadata, Result & result) const;
};

}
