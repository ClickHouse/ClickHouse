#pragma once

#include <Databases/DatabaseSchemaWAL.h>
#include <Databases/UDT/AtomicDatabaseSchemaMutationStorage.h>
#include <Databases/UDT/AuthorityRoot.h>
#include <Databases/UDT/AuthorityVerificationStamp.h>

#include <DataTypes/UDT/BoundObjectTypeReferences.h>
#include <DataTypes/UDT/TableColumnTypeBindings.h>
#include <Interpreters/UDT/DictionaryAttributeTypeBindings.h>
#include <Interpreters/UDT/ViewOutputTypeBindings.h>

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>

#include <optional>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

enum class DependentObjectMutationKind : UInt8
{
    Drop = 1,
    Rename = 2,
    Alter = 3,
    AlterAdmission = 4,
};

struct DependentObjectMutationRequest
{
    DependentObjectMutationKind kind{};
    SchemaObjectID object;
    UInt64 transaction_id = 0;
    UInt64 expected_database_catalog_epoch = 0;
    AtomicDatabaseSchemaMutationDependentObjectImage before_image;
    String physical_before_object_name;
    String physical_before_canonical_metadata_bytes;
    String after_object_name;
    NamesAndTypesList physical_columns;
    String after_canonical_metadata_bytes;
    std::optional<PersistedTypeReferences> after_persisted_references;
    std::vector<SchemaObjectID> after_object_dependencies;
};

struct DependentObjectMutationPlannerLimits
{
    DependentObjectMutationPlannerLimits();

    AuthorityRootBuildLimits authority_root;
    DatabaseSchemaWALLimits schema_wal;
    TableColumnTypeBindingLimits table_columns;
    ViewOutputTypeBindingLimits view_outputs;
    DictionaryAttributeTypeBindingLimits dictionary_attributes;
    BoundObjectTypeReferencesLimits bound_references;
};

class DependentObjectMutationPlannerError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidRequest,
        DatabaseMismatch,
        ExpectedEpochMismatch,
        ObjectNotFound,
        StaleImage,
        InvalidBindings,
        RemainingDependent,
        LimitExceeded,
        InvalidTransition,
    };

    DependentObjectMutationPlannerError(Code code_, std::string_view message);

    const Code code;
};

class PreparedDependentObjectMutation final
{
public:
    PreparedDependentObjectMutation(const PreparedDependentObjectMutation &) = delete;
    PreparedDependentObjectMutation & operator=(const PreparedDependentObjectMutation &) = delete;
    PreparedDependentObjectMutation(PreparedDependentObjectMutation &&) noexcept = default;
    PreparedDependentObjectMutation & operator=(PreparedDependentObjectMutation &&) noexcept = default;

    DependentObjectMutationKind getKind() const noexcept { return kind; }
    const AuthorityRoot & getReplacementRoot() const noexcept { return *replacement_root; }
    const DatabaseSchemaWALValidatedTransition & getValidatedTransition() const noexcept { return transition; }
    const BoundObjectTypeReferences::Ptr & getBoundUDTReferences() const noexcept { return bound_references; }
    const std::optional<SidecarExpectationRecord> & getSidecarExpectation() const noexcept { return expectation; }
    const AuthorityVerificationStamp::Ptr & getVerificationStamp() const noexcept { return verification_stamp; }
    bool wasPlannedFrom(const AuthorityRoot & root) const noexcept { return planning_root == &root; }
    AuthorityRoot::Ptr releaseReplacementRoot() noexcept { return std::move(replacement_root); }

private:
    PreparedDependentObjectMutation(
        DependentObjectMutationKind kind_,
        const AuthorityRoot * planning_root_,
        AuthorityRoot::Ptr replacement_root_,
        DatabaseSchemaWALValidatedTransition transition_,
        BoundObjectTypeReferences::Ptr bound_references_,
        std::optional<SidecarExpectationRecord> expectation_,
        AuthorityVerificationStamp::Ptr verification_stamp_);

    friend class DependentObjectMutationPlanner;

    DependentObjectMutationKind kind;
    const AuthorityRoot * planning_root;
    AuthorityRoot::Ptr replacement_root;
    DatabaseSchemaWALValidatedTransition transition;
    BoundObjectTypeReferences::Ptr bound_references;
    std::optional<SidecarExpectationRecord> expectation;
    AuthorityVerificationStamp::Ptr verification_stamp;
};

class DependentObjectMutationPlanner final
{
public:
    [[nodiscard]] static PreparedDependentObjectMutation plan(
        const AuthorityRoot & current_root,
        DependentObjectMutationRequest request,
        const DependentObjectMutationPlannerLimits & limits = {});

private:
    DependentObjectMutationPlanner() = delete;
};

}
