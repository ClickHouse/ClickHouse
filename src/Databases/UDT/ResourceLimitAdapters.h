#pragma once

#include <Databases/UDT/AuthorityIntegrityVerifier.h>
#include <Databases/UDT/AuthorityVerificationSchedule.h>
#include <Databases/UDT/PhysicalizationTokenStore.h>

#include <DataTypes/UDT/ResourceLimits.h>

namespace DB::UDT
{

/// Lowers every token-store counter whose deterministic charge identity is
/// shared with the common resource-limit contract. Entropy retry limits are
/// component-local and retain their reviewed default.
PhysicalizationTokenStoreLimits makePhysicalizationTokenStoreLimits(const EffectiveResourceLimits & limits);
/// Keeps canonical decoders and every indivisible existing target at immutable
/// implementation bounds. Mutable verification layers govern admission of new
/// rooted artifacts and policies; they must never deactivate integrity work.
AuthorityIntegrityVerifierLimits makeAuthorityIntegrityVerifierLimits(const EffectiveResourceLimits & limits);
/// Maps the complete mutable verification admission tuple. Callers which own
/// an already-published immutable root may widen only the rooted escape and
/// exact planning requirements needed by that root; aggregate batch policy
/// remains at the effective minimum.
AuthorityVerificationScheduleLimits makeAuthorityVerificationScheduleLimits(const EffectiveResourceLimits & limits);

}
