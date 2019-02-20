#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Names.h>
#include <Storages/ColumnDefault.h>
#include <Core/Block.h>
#include <Storages/ColumnCodec.h>


namespace DB
{

/// key-values column_name, column_comment. column_comment should be non empty.
using ColumnComments = std::unordered_map<std::string, String>;
using ColumnTTLs = std::unordered_map<std::string, ASTPtr>;

struct ColumnsDescription
{
    NamesAndTypesList ordinary;
    NamesAndTypesList materialized;
    NamesAndTypesList aliases;
    ColumnDefaults defaults;
    ColumnComments comments;
    ColumnCodecs codecs;
    ColumnTTLs ttl_asts;

    ColumnsDescription() = default;

    ColumnsDescription(
        NamesAndTypesList ordinary_,
        NamesAndTypesList materialized_,
        NamesAndTypesList aliases_,
        ColumnDefaults defaults_,
        ColumnComments comments_,
        ColumnCodecs codecs_,
        ColumnTTLs ttl_asts_)
        : ordinary(std::move(ordinary_))
        , materialized(std::move(materialized_))
        , aliases(std::move(aliases_))
        , defaults(std::move(defaults_))
        , comments(std::move(comments_))
        , codecs(std::move(codecs_))
        , ttl_asts(std::move(ttl_asts_))
    {}

    explicit ColumnsDescription(NamesAndTypesList ordinary_) : ordinary(std::move(ordinary_)) {}

    bool operator==(const ColumnsDescription & other) const
    {
        return ordinary == other.ordinary
            && materialized == other.materialized
            && aliases == other.aliases
            && defaults == other.defaults
            && comments == other.comments
            && codecs == other.codecs
            && ttl_asts == other.ttl_asts;
    }

    bool operator!=(const ColumnsDescription & other) const { return !(*this == other); }

    /// ordinary + materialized.
    NamesAndTypesList getAllPhysical() const;

    /// ordinary + materialized + aliases.
    NamesAndTypesList getAll() const;

    Names getNamesOfPhysical() const;

    NameAndTypePair getPhysical(const String & column_name) const;

    bool hasPhysical(const String & column_name) const;

    String toString() const;

    CompressionCodecPtr getCodecOrDefault(const String & column_name, CompressionCodecPtr default_codec) const;

    static ColumnsDescription parse(const String & str);

    static const ColumnsDescription * loadFromContext(const Context & context, const String & db, const String & table);
};

}
