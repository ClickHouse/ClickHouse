#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AZURE_BLOB_STORAGE

#include <Disks/IDiskRemote.h>

#if defined(__clang__)
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Winconsistent-missing-destructor-override"
#    pragma clang diagnostic ignored "-Wdeprecated-copy-dtor"
#    pragma clang diagnostic ignored "-Wextra-semi"
#    ifdef HAS_SUGGEST_DESTRUCTOR_OVERRIDE
#        pragma clang diagnostic ignored "-Wsuggest-destructor-override"
#    endif
#    ifdef HAS_RESERVED_IDENTIFIER
#        pragma clang diagnostic ignored "-Wreserved-identifier"
#    endif
#endif

#include <azure/storage/blobs.hpp>

#if defined(__clang__)
#    pragma clang diagnostic pop
#endif

namespace DB
{

std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> getBlobContainerClient(
    const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

}

#endif
