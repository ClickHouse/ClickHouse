#include "config.h"

#if USE_FILELOG

#include <Storages/FileLog/StorageFileLog.h>

#include <gtest/gtest.h>

#include <filesystem>

using namespace DB;

TEST(StorageFileLog, ResolveRelativePath)
{
    const String user_files = std::filesystem::current_path() / "user_files" / "";

    EXPECT_EQ(resolveFileLogPath("nginx/", user_files), user_files + "nginx/");
    EXPECT_EQ(resolveFileLogPath("/var/log/nginx/", user_files), "/var/log/nginx/");
    /// Already inside `user_files_path` from the working directory.
    EXPECT_EQ(resolveFileLogPath("user_files/nginx/", user_files), "user_files/nginx/");
    EXPECT_EQ(resolveFileLogPath("./user_files/nginx/", user_files), "./user_files/nginx/");
    /// `clickhouse-local` has no `user_files_path`.
    EXPECT_EQ(resolveFileLogPath("nginx/", ""), "nginx/");
}

#endif
