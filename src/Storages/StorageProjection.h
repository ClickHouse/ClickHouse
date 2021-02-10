#pragma once

#include <ext/shared_ptr_helper.h>

#include <Interpreters/Context.h>
#include <Storages/IStorage.h>


namespace DB
{
/** A table that represents a projection.
  */
class StorageProjection final : public ext::shared_ptr_helper<StorageProjection>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageProjection>;

public:
    std::string getName() const override { return "Projection"; }

    /// The check is delayed to the read method. It checks the support of the tables used.
    bool supportsSampling() const override { return true; }
    bool supportsPrewhere() const override { return true; }
    bool supportsIndexForIn() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand, const Context &, const StorageMetadataPtr & metadata_snapshot) const override;

    NamesAndTypesList getVirtuals() const override;

private:
    StorageID source_table_id;
    String projection_name;
    const Context & global_context;

protected:
    StorageProjection(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const StorageID & source_table_id_,
        const String & projection_name_,
        const Context & context_);
};

}
