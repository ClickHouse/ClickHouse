#pragma once

#include <Databases/UDT/AuthorityVerificationStamp.h>

#include <DataTypes/UDT/BoundObjectTypeReferences.h>

#include <string_view>
#include <vector>

namespace DB::UDT
{

class AuthorityRoot;

/// Canonical direct definition identities covered by both the persisted
/// sidecar verifier and an immutable bound object image. The result retains no
/// catalog handles and is suitable for a compact continuation stamp.
[[nodiscard]] std::vector<DefinitionIdentity> collectAuthorityVerificationRequiredDefinitions(
    const BoundObjectTypeReferences & bound_references,
    UInt64 maximum_required_definitions = AuthorityVerificationStampLimits{}.maximum_required_definitions);

/// Runs the exact-root integrity verifier over one already-canonical sidecar
/// and its already-bound live image, then creates the compact proof used by
/// quarantine read-continuation admission. It performs no publication.
[[nodiscard]] AuthorityVerificationStamp::Ptr verifyAndCreateAuthorityVerificationStamp(
    const AuthorityRoot & root,
    const SidecarExpectationRecord & expectation,
    std::string_view canonical_sidecar_bytes,
    const BoundObjectTypeReferences & bound_references,
    const AuthorityIntegrityVerifierLimits & verifier_limits = {},
    const AuthorityVerificationStampLimits & stamp_limits = {});

/// Reanchors an already-verified immutable object image to a later exact root
/// after directly proving that its rooted expectation, complete definition
/// closure, checked definition bodies, and graph edges are unchanged. This is
/// bounded by the stamp closure and does not trust an unrooted caller delta.
[[nodiscard]] AuthorityVerificationStamp::Ptr validateAndRebaseAuthorityVerificationStamp(
    const AuthorityRoot & root,
    const SidecarExpectationRecord & expectation,
    const BoundObjectTypeReferences & bound_references,
    const AuthorityVerificationStamp & previous_stamp,
    const AuthorityVerificationStampLimits & stamp_limits = {});

}
