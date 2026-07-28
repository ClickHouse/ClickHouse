#include <gtest/gtest.h>

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <IO/AzureBlobStorage/copyAzureBlobStorageFile.h>

using namespace DB;

/// Regression guard: Azure server-side copy (CopyFromUri) copies the whole source blob, so it must
/// not be used for a ranged (offset>0) copy -- that path must fall back to read+write.
TEST(CopyAzureBlobStorageFile, RangedCopyMustNotUseNativeCopy)
{
    /// The bug: a ranged copy used native whole-blob copy and silently copied the entire source.
    EXPECT_FALSE(azureCopyShouldTryNativeCopy(/*use_native_copy=*/ true, /*offset=*/ 1));
    EXPECT_FALSE(azureCopyShouldTryNativeCopy(true, 4096));

    /// A full-object copy still uses native copy when it is enabled.
    EXPECT_TRUE(azureCopyShouldTryNativeCopy(true, 0));

    /// Native copy disabled -> never native, regardless of offset.
    EXPECT_FALSE(azureCopyShouldTryNativeCopy(false, 0));
    EXPECT_FALSE(azureCopyShouldTryNativeCopy(false, 4096));
}

#endif
