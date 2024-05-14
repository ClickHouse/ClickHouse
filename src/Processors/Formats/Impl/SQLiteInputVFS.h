#pragma once

#include "config.h"

#if USE_SQLITE

#    include <sqlite3.h>

#    include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#    include <arrow/result.h>

#    define ORIGVFS(p) (static_cast<sqlite3_vfs *>((p)->pAppData))

class RandomAccessSQLiteFile : public sqlite3_file
{
public:
    arrow::io::RandomAccessFile * ptr;
};

static int spmemfileClose(sqlite3_file * file);
static int spmemfileRead(sqlite3_file * file, void * buffer, int len, sqlite3_int64 offset);
static int spmemfileWrite(sqlite3_file * file, const void * buffer, int len, sqlite3_int64 offset);
static int spmemfileTruncate(sqlite3_file * file, sqlite3_int64 size);
static int spmemfileSync(sqlite3_file * file, int flags);
static int spmemfileFileSize(sqlite3_file * file, sqlite3_int64 * size);
static int spmemfileLock(sqlite3_file * file, int type);
static int spmemfileUnlock(sqlite3_file * file, int type);
static int spmemfileCheckReservedLock(sqlite3_file * file, int * result);
static int spmemfileFileControl(sqlite3_file * file, int op, void * arg);
static int spmemfileSectorSize(sqlite3_file * file);
static int spmemfileDeviceCharacteristics(sqlite3_file * file);
static int memShmMap(sqlite3_file *, int iPg, int pgsz, int, void volatile **);
static int memShmLock(sqlite3_file *, int offset, int n, int flags);
static void memShmBarrier(sqlite3_file *);
static int memShmUnmap(sqlite3_file *, int deleteFlag);
static int memFetch(sqlite3_file *, sqlite3_int64 iOfst, int iAmt, void ** pp);
static int memUnfetch(sqlite3_file *, sqlite3_int64 iOfst, void * p);

static int memOpen(sqlite3_vfs *, const char *, sqlite3_file *, int, int *);
static int memDelete(sqlite3_vfs *, const char * zName, int syncDir);
static int memAccess(sqlite3_vfs *, const char * zName, int flags, int *);
static int memFullPathname(sqlite3_vfs *, const char * zName, int, char * zOut);
static void * memDlOpen(sqlite3_vfs *, const char * zFilename);
static void memDlError(sqlite3_vfs *, int nByte, char * zErrMsg);
static void (*memDlSym(sqlite3_vfs * pVfs, void * p, const char * zSym))(void);
static void memDlClose(sqlite3_vfs *, void *);
static int memRandomness(sqlite3_vfs *, int nByte, char * zOut);
static int memSleep(sqlite3_vfs *, int microseconds);
static int memCurrentTime(sqlite3_vfs *, double *);
static int memGetLastError(sqlite3_vfs *, int, char *);
static int memCurrentTimeInt64(sqlite3_vfs *, sqlite3_int64 *);
static int memSetSystemCall(sqlite3_vfs *, const char * zName, sqlite3_syscall_ptr);
static sqlite3_syscall_ptr memGetSystemCall(sqlite3_vfs *, const char * zName);
const char * memNextSystemCall(sqlite3_vfs *, const char * zName);

static sqlite3_io_methods mem_io_methods = {
    1, /* iVersion */
    spmemfileClose, /* xClose */
    spmemfileRead, /* xRead */
    spmemfileWrite, /* xWrite */
    spmemfileTruncate, /* xTruncate */
    spmemfileSync, /* xSync */
    spmemfileFileSize, /* xFileSize */
    spmemfileLock, /* xLock */
    spmemfileUnlock, /* xUnlock */
    spmemfileCheckReservedLock, /* xCheckReservedLock */
    spmemfileFileControl, /* xFileControl */
    spmemfileSectorSize, /* xSectorSize */
    spmemfileDeviceCharacteristics, /* xDeviceCharacteristics */
    memShmMap, /* xShmMap */
    memShmLock, /* xShmLock */
    memShmBarrier, /* xShmBarrier */
    memShmUnmap, /* xShmUnmap */
    memFetch, /* xFetch */
    memUnfetch /* xUnfetch */
};

static sqlite3_vfs mem_vfs = {
    1, /* iVersion */
    sizeof(RandomAccessSQLiteFile), /* szOsFile */
    1024, /* mxPathname */
    nullptr, /* pNext */
    "ch_read_vfs", /* zName */
    nullptr, /* pAppData (set in initMemVFS) */
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
    memSetSystemCall, /* xSetSystemCall */
    memGetSystemCall, /* xGetSystemCall */
    memNextSystemCall /* xNextSystemCall */
};

static std::atomic<int> is_inited = 0;

int spmemfileClose(sqlite3_file *)
{
    return SQLITE_OK;
}

int spmemfileRead(sqlite3_file * file, void * buffer, int len, sqlite3_int64 offset)
{
    RandomAccessSQLiteFile * memfile = static_cast<RandomAccessSQLiteFile *>(file);

    if (*(memfile->ptr->ReadAt(offset, len, buffer)) != len)
    {
        return SQLITE_IOERR_SHORT_READ;
    }

    return SQLITE_OK;
}

int spmemfileWrite(sqlite3_file *, const void *, int, sqlite3_int64)
{
    return SQLITE_READONLY;
}

int spmemfileTruncate(sqlite3_file *, sqlite3_int64)
{
    return SQLITE_OK;
}

int spmemfileSync(sqlite3_file *, int)
{
    return SQLITE_OK;
}

int spmemfileFileSize(sqlite3_file * file, sqlite3_int64 * size)
{
    RandomAccessSQLiteFile * memfile = static_cast<RandomAccessSQLiteFile *>(file);

    *size = *(memfile->ptr->GetSize());

    return SQLITE_OK;
}

int spmemfileLock(sqlite3_file *, int)
{
    return SQLITE_OK;
}

int spmemfileUnlock(sqlite3_file *, int)
{
    return SQLITE_OK;
}

int spmemfileCheckReservedLock(sqlite3_file *, int * result)
{
    *result = 0;
    return SQLITE_OK;
}

int spmemfileFileControl(sqlite3_file *, int op, void *)
{
    int rc = SQLITE_NOTFOUND;
    if (op == SQLITE_FCNTL_VFSNAME)
    {
        rc = SQLITE_OK;
    }
    return rc;
}

int spmemfileSectorSize(sqlite3_file *)
{
    return 0;
}

int spmemfileDeviceCharacteristics(sqlite3_file *)
{
    return 0;
}

int memShmMap(sqlite3_file *, int, int, int, void volatile **)
{
    return SQLITE_IOERR_SHMMAP;
}

static int memShmLock(sqlite3_file *, int, int, int)
{
    return SQLITE_IOERR_SHMLOCK;
}

static void memShmBarrier(sqlite3_file *)
{
    return;
}

static int memShmUnmap(sqlite3_file *, int)
{
    return SQLITE_OK;
}

static int memFetch(sqlite3_file *, sqlite3_int64, int, void **)
{
    return SQLITE_OK;
}

static int memUnfetch(sqlite3_file *, sqlite3_int64, void *)
{
    return SQLITE_OK;
}


//////////////////////////////////

static int memOpen(
    sqlite3_vfs * /*pVfs*/, const char * zName, sqlite3_file * pFile, int /*flags*/, int * /*pOutFlags*/
)
{
    RandomAccessSQLiteFile * c = reinterpret_cast<RandomAccessSQLiteFile *>(pFile);
    c->ptr = reinterpret_cast<arrow::io::RandomAccessFile *>(std::stoll(zName, nullptr, 16));
    if (c->ptr == nullptr)
    {
        return SQLITE_CANTOPEN;
    }
    pFile->pMethods = &mem_io_methods;
    return SQLITE_OK;
}

static int memDelete(sqlite3_vfs * /*pVfs*/, const char * /*zPath*/, int /*dirSync*/)
{
    return SQLITE_IOERR_DELETE;
}

static int memAccess(sqlite3_vfs * /*pVfs*/, const char * /*zPath*/, int /*flags*/, int * pResOut)
{
    *pResOut = 0;
    return SQLITE_OK;
}

static int memFullPathname(sqlite3_vfs * /*pVfs*/, const char * zPath, int nOut, char * zOut)
{
    sqlite3_snprintf(nOut, zOut, "%s", zPath);
    return SQLITE_OK;
}

static void * memDlOpen(sqlite3_vfs * pVfs, const char * zPath)
{
    return ORIGVFS(pVfs)->xDlOpen(ORIGVFS(pVfs), zPath);
}

static void memDlError(sqlite3_vfs * pVfs, int nByte, char * zErrMsg)
{
    ORIGVFS(pVfs)->xDlError(ORIGVFS(pVfs), nByte, zErrMsg);
}

static void (*memDlSym(sqlite3_vfs * pVfs, void * p, const char * zSym))(void)
{
    return ORIGVFS(pVfs)->xDlSym(ORIGVFS(pVfs), p, zSym);
}

static void memDlClose(sqlite3_vfs * pVfs, void * pHandle)
{
    ORIGVFS(pVfs)->xDlClose(ORIGVFS(pVfs), pHandle);
}

static int memRandomness(sqlite3_vfs * pVfs, int nByte, char * zBufOut)
{
    return ORIGVFS(pVfs)->xRandomness(ORIGVFS(pVfs), nByte, zBufOut);
}

static int memSleep(sqlite3_vfs * pVfs, int nMicro)
{
    return ORIGVFS(pVfs)->xSleep(ORIGVFS(pVfs), nMicro);
}

static int memCurrentTime(sqlite3_vfs * pVfs, double * pTimeOut)
{
    return ORIGVFS(pVfs)->xCurrentTime(ORIGVFS(pVfs), pTimeOut);
}

static int memGetLastError(sqlite3_vfs * pVfs, int a, char * b)
{
    return ORIGVFS(pVfs)->xGetLastError(ORIGVFS(pVfs), a, b);
}

static int memCurrentTimeInt64(sqlite3_vfs * pVfs, sqlite3_int64 * p)
{
    return ORIGVFS(pVfs)->xCurrentTimeInt64(ORIGVFS(pVfs), p);
}

static int memSetSystemCall(sqlite3_vfs *, const char *, sqlite3_syscall_ptr)
{
    return SQLITE_OK;
}

static sqlite3_syscall_ptr memGetSystemCall(sqlite3_vfs *, const char *)
{
    return nullptr;
}

const char * memNextSystemCall(sqlite3_vfs *, const char *)
{
    return nullptr;
}

static int initMemVFS()
{
    int zero = 0;
    if (is_inited.compare_exchange_strong(zero, 1))
    {
        mem_vfs.pAppData = sqlite3_vfs_find(nullptr);
        if (mem_vfs.pAppData == nullptr)
            return SQLITE_ERROR;
        return sqlite3_vfs_register(&mem_vfs, 0);
    }
    return SQLITE_OK;
}

#endif
