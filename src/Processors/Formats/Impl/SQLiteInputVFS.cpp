#include <Processors/Formats/Impl/SQLiteInputVFS.h>

#if USE_SQLITE

#include <mutex>

#include <base/hex.h>
#include <Common/Exception.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>

#include <sqlite3.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SQLITE_ENGINE_ERROR;
}

const char * const sqlite_read_vfs_name = "ch_read_vfs";

namespace
{

#define ORIGVFS(p) (static_cast<sqlite3_vfs *>((p)->pAppData))

/// An sqlite3_file backed by an arrow RandomAccessFile (ClickHouse ReadBuffer).
class RandomAccessSQLiteFile : public sqlite3_file
{
public:
    arrow::io::RandomAccessFile * ptr;
};

extern sqlite3_io_methods mem_io_methods;

int memClose(sqlite3_file *)
{
    return SQLITE_OK;
}

int memRead(sqlite3_file * file, void * buffer, int len, sqlite3_int64 offset)
{
    auto * memfile = static_cast<RandomAccessSQLiteFile *>(file);

    auto bytes_read = memfile->ptr->ReadAt(offset, len, buffer);
    if (!bytes_read.ok())
        return SQLITE_IOERR_READ;

    if (*bytes_read != len)
        return SQLITE_IOERR_SHORT_READ;

    return SQLITE_OK;
}

int memWrite(sqlite3_file *, const void *, int, sqlite3_int64)
{
    return SQLITE_READONLY;
}

int memTruncate(sqlite3_file *, sqlite3_int64)
{
    return SQLITE_OK;
}

int memSync(sqlite3_file *, int)
{
    return SQLITE_OK;
}

int memFileSize(sqlite3_file * file, sqlite3_int64 * size)
{
    auto * memfile = static_cast<RandomAccessSQLiteFile *>(file);

    auto file_size = memfile->ptr->GetSize();
    if (!file_size.ok())
        return SQLITE_IOERR_FSTAT;

    *size = *file_size;
    return SQLITE_OK;
}

int memLock(sqlite3_file *, int)
{
    return SQLITE_OK;
}

int memUnlock(sqlite3_file *, int)
{
    return SQLITE_OK;
}

int memCheckReservedLock(sqlite3_file *, int * result)
{
    *result = 0;
    return SQLITE_OK;
}

int memFileControl(sqlite3_file *, int op, void *)
{
    int rc = SQLITE_NOTFOUND;
    if (op == SQLITE_FCNTL_VFSNAME)
        rc = SQLITE_OK;
    return rc;
}

int memSectorSize(sqlite3_file *)
{
    return 0;
}

int memDeviceCharacteristics(sqlite3_file *)
{
    return 0;
}

int memShmMap(sqlite3_file *, int, int, int, void volatile **)
{
    return SQLITE_IOERR_SHMMAP;
}

int memShmLock(sqlite3_file *, int, int, int)
{
    return SQLITE_IOERR_SHMLOCK;
}

void memShmBarrier(sqlite3_file *)
{
}

int memShmUnmap(sqlite3_file *, int)
{
    return SQLITE_OK;
}

int memFetch(sqlite3_file *, sqlite3_int64, int, void **)
{
    return SQLITE_OK;
}

int memUnfetch(sqlite3_file *, sqlite3_int64, void *)
{
    return SQLITE_OK;
}

int memOpen(sqlite3_vfs *, const char * zName, sqlite3_file * pFile, int, int *)
{
    auto * c = static_cast<RandomAccessSQLiteFile *>(pFile);
    c->ptr = reinterpret_cast<arrow::io::RandomAccessFile *>(unhexUInt<uintptr_t>(zName));
    if (c->ptr == nullptr)
        return SQLITE_CANTOPEN;
    pFile->pMethods = &mem_io_methods;
    return SQLITE_OK;
}

int memDelete(sqlite3_vfs *, const char *, int)
{
    return SQLITE_IOERR_DELETE;
}

int memAccess(sqlite3_vfs *, const char *, int, int * pResOut)
{
    *pResOut = 0;
    return SQLITE_OK;
}

int memFullPathname(sqlite3_vfs *, const char * zPath, int nOut, char * zOut)
{
    sqlite3_snprintf(nOut, zOut, "%s", zPath);
    return SQLITE_OK;
}

void * memDlOpen(sqlite3_vfs * pVfs, const char * zPath)
{
    return ORIGVFS(pVfs)->xDlOpen(ORIGVFS(pVfs), zPath);
}

void memDlError(sqlite3_vfs * pVfs, int nByte, char * zErrMsg)
{
    ORIGVFS(pVfs)->xDlError(ORIGVFS(pVfs), nByte, zErrMsg);
}

void (*memDlSym(sqlite3_vfs * pVfs, void * p, const char * zSym))(void)
{
    return ORIGVFS(pVfs)->xDlSym(ORIGVFS(pVfs), p, zSym);
}

void memDlClose(sqlite3_vfs * pVfs, void * pHandle)
{
    ORIGVFS(pVfs)->xDlClose(ORIGVFS(pVfs), pHandle);
}

int memRandomness(sqlite3_vfs * pVfs, int nByte, char * zBufOut)
{
    return ORIGVFS(pVfs)->xRandomness(ORIGVFS(pVfs), nByte, zBufOut);
}

int memSleep(sqlite3_vfs * pVfs, int nMicro)
{
    return ORIGVFS(pVfs)->xSleep(ORIGVFS(pVfs), nMicro);
}

int memCurrentTime(sqlite3_vfs * pVfs, double * pTimeOut)
{
    return ORIGVFS(pVfs)->xCurrentTime(ORIGVFS(pVfs), pTimeOut);
}

int memGetLastError(sqlite3_vfs * pVfs, int a, char * b)
{
    return ORIGVFS(pVfs)->xGetLastError(ORIGVFS(pVfs), a, b);
}

int memCurrentTimeInt64(sqlite3_vfs * pVfs, sqlite3_int64 * p)
{
    return ORIGVFS(pVfs)->xCurrentTimeInt64(ORIGVFS(pVfs), p);
}

sqlite3_io_methods mem_io_methods = {
    1, /* iVersion */
    memClose, /* xClose */
    memRead, /* xRead */
    memWrite, /* xWrite */
    memTruncate, /* xTruncate */
    memSync, /* xSync */
    memFileSize, /* xFileSize */
    memLock, /* xLock */
    memUnlock, /* xUnlock */
    memCheckReservedLock, /* xCheckReservedLock */
    memFileControl, /* xFileControl */
    memSectorSize, /* xSectorSize */
    memDeviceCharacteristics, /* xDeviceCharacteristics */
    memShmMap, /* xShmMap */
    memShmLock, /* xShmLock */
    memShmBarrier, /* xShmBarrier */
    memShmUnmap, /* xShmUnmap */
    memFetch, /* xFetch */
    memUnfetch /* xUnfetch */
};

sqlite3_vfs mem_vfs = {
    1, /* iVersion */
    sizeof(RandomAccessSQLiteFile), /* szOsFile */
    1024, /* mxPathname */
    nullptr, /* pNext */
    sqlite_read_vfs_name, /* zName */
    nullptr, /* pAppData (set in initSQLiteReadVFS) */
    memOpen, /* xOpen */
    memDelete, /* xDelete */
    memAccess, /* xAccess */
    memFullPathname, /* xFullPathname */
    memDlOpen, /* xDlOpen */
    memDlError, /* xDlError */
    memDlSym, /* xDlSym */
    memDlClose, /* xDlClose */
    memRandomness, /* xRandomness */
    memSleep, /* xSleep */
    memCurrentTime, /* xCurrentTime */
    memGetLastError, /* xGetLastError */
    memCurrentTimeInt64, /* xCurrentTimeInt64 */
    nullptr, /* xSetSystemCall */
    nullptr, /* xGetSystemCall */
    nullptr /* xNextSystemCall */
};

std::once_flag vfs_init_flag;

}

void initSQLiteReadVFS()
{
    std::call_once(vfs_init_flag, []
    {
        mem_vfs.pAppData = sqlite3_vfs_find(nullptr);
        if (mem_vfs.pAppData == nullptr)
            throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "Cannot find the default SQLite VFS");

        int status = sqlite3_vfs_register(&mem_vfs, 0);
        if (status != SQLITE_OK)
            throw Exception(
                ErrorCodes::SQLITE_ENGINE_ERROR,
                "Cannot register SQLite VFS. Error status: {}. Message: {}",
                status,
                sqlite3_errstr(status));
    });
}

std::string encodeSQLiteVFSFileName(arrow::io::RandomAccessFile * file)
{
    return getHexUIntLowercase(reinterpret_cast<uintptr_t>(file));
}

}

#endif
