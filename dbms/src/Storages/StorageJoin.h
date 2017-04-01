#pragma once

#include <ext/shared_ptr_helper.hpp>

#include <Storages/StorageSet.h>
#include <Parsers/ASTTablesInSelectQuery.h>


namespace DB
{

class Join;
using JoinPtr = std::shared_ptr<Join>;


/** Позволяет сохранить состояние для последующего использования в правой части JOIN.
  * При вставке в таблицу, данные будут вставлены в состояние,
  *  а также записаны в файл-бэкап, для восстановления после перезапуска.
  * Чтение из таблицы напрямую невозможно - возможно лишь указание в правой части JOIN.
  *
  * При использовании, JOIN должен быть соответствующего типа (ANY|ALL LEFT|INNER ...).
  */
class StorageJoin : private ext::shared_ptr_helper<StorageJoin>, public StorageSetOrJoinBase
{
friend class ext::shared_ptr_helper<StorageJoin>;

public:
    static StoragePtr create(
        const String & path_,
        const String & name_,
        const Names & key_names_,
        ASTTableJoin::Kind kind_, ASTTableJoin::Strictness strictness_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_)
    {
        return ext::shared_ptr_helper<StorageJoin>::make_shared(
            path_, name_, key_names_, kind_, strictness_,
            columns_, materialized_columns_, alias_columns_, column_defaults_
        );
    }

    String getName() const override { return "Join"; }

    /// Получить доступ к внутренностям.
    JoinPtr & getJoin() { return join; }

    /// Убедиться, что структура данных подходит для осуществления JOIN такого типа.
    void assertCompatible(ASTTableJoin::Kind kind_, ASTTableJoin::Strictness strictness_) const;

private:
    const Names & key_names;
    ASTTableJoin::Kind kind;                    /// LEFT | INNER ...
    ASTTableJoin::Strictness strictness;        /// ANY | ALL

    JoinPtr join;

    StorageJoin(
        const String & path_,
        const String & name_,
        const Names & key_names_,
        ASTTableJoin::Kind kind_, ASTTableJoin::Strictness strictness_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_);

    void insertBlock(const Block & block) override;
    size_t getSize() const override;
};

}
