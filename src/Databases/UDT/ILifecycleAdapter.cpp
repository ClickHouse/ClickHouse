#include <Databases/UDT/ILifecycleAdapter.h>

#include <Common/Exception.h>

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace DB::UDT
{
namespace
{

class UnsupportedLifecycleAdapter final : public ILifecycleAdapter
{
public:
    const TypeAuthorityCapabilities & getCapabilities() const noexcept override
    {
        return getUnsupportedAuthorityAdapter().getCapabilities();
    }

    UUID getDatabaseUUID() const noexcept override { return UUIDHelpers::Nil; }

    void requireCapabilities(TypeAuthorityCapabilityMask required, std::string_view operation) const override
    {
        if (required != 0)
            unsupported(operation);
    }

    std::unique_ptr<const ILifecycleSnapshot> acquireSnapshot() const override { unsupported("user-defined type introspection"); }

    void createOrAttach(const ASTCreateTypeQuery &, const LifecycleActor &) override { unsupported("CREATE or ATTACH TYPE"); }

    void rename(const ASTRenameTypeQuery &, const LifecycleActor &) override { unsupported("ALTER TYPE RENAME"); }

    void comment(const ASTAlterTypeCommentQuery &, const LifecycleActor &) override { unsupported("ALTER TYPE COMMENT"); }

    void dropRestrict(const ASTDropTypeQuery &, const LifecycleActor &) override { unsupported("DROP TYPE RESTRICT"); }

    PhysicalizationDryRunResult
    physicalizationDryRun(PhysicalizationSelector, const LifecycleActor &, const IPhysicalizationDryRunAuthorization &) override
    {
        unsupported("PHYSICALIZE TYPE REFERENCES DRY RUN");
    }

    void physicalizationApply(std::string_view, const LifecycleActor &, const IPhysicalizationApplyAuthorization &) override
    {
        unsupported("PHYSICALIZE TYPE REFERENCES APPLY");
    }

private:
    [[noreturn]] static void unsupported(std::string_view operation)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "User-defined type authority does not support {}", operation);
    }
};

}

ILifecycleAdapter & getUnsupportedLifecycleAdapter() noexcept
{
    static UnsupportedLifecycleAdapter adapter;
    return adapter;
}

}
