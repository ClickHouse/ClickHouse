/**
 * Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#ifndef DICTBUILDER_H_001
#define DICTBUILDER_H_001

#if defined (__cplusplus)
extern "C" {
#endif


/*======  Dependencies  ======*/
#include <stddef.h>  /* size_t */


/*======  Export for Windows  ======*/
/*!
*  ZSTD_DLL_EXPORT :
*  Enable exporting of functions when building a Windows DLL
*/
#if defined(_WIN32) && defined(ZSTD_DLL_EXPORT) && (ZSTD_DLL_EXPORT==1)
#  define ZDICTLIB_API __declspec(dllexport)
#else
#  define ZDICTLIB_API
#endif


/*! ZDICT_trainFromBuffer() :
    Train a dictionary from an array of samples.
    Samples must be stored concatenated in a single flat buffer `samplesBuffer`,
    supplied with an array of sizes `samplesSizes`, providing the size of each sample, in order.
    The resulting dictionary will be saved into `dictBuffer`.
    @return : size of dictionary stored into `dictBuffer` (<= `dictBufferCapacity`)
              or an error code, which can be tested with ZDICT_isError().
    Tips : In general, a reasonable dictionary has a size of ~ 100 KB.
           It's obviously possible to target smaller or larger ones, just by specifying different `dictBufferCapacity`.
           In general, it's recommended to provide a few thousands samples, but this can vary a lot.
           It's recommended that total size of all samples be about ~x100 times the target size of dictionary.
*/
ZDICTLIB_API size_t ZDICT_trainFromBuffer(void* dictBuffer, size_t dictBufferCapacity,
                       const void* samplesBuffer, const size_t* samplesSizes, unsigned nbSamples);


/*======   Helper functions   ======*/
ZDICTLIB_API unsigned ZDICT_getDictID(const void* dictBuffer, size_t dictSize);  /**< extracts dictID; @return zero if error (not a valid dictionary) */
ZDICTLIB_API unsigned ZDICT_isError(size_t errorCode);
ZDICTLIB_API const char* ZDICT_getErrorName(size_t errorCode);



#ifdef ZDICT_STATIC_LINKING_ONLY

/* ====================================================================================
 * The definitions in this section are considered experimental.
 * They should never be used with a dynamic library, as they may change in the future.
 * They are provided for advanced usages.
 * Use them only in association with static linking.
 * ==================================================================================== */

typedef struct {
    unsigned selectivityLevel;   /* 0 means default; larger => select more => larger dictionary */
    int      compressionLevel;   /* 0 means default; target a specific zstd compression level */
    unsigned notificationLevel;  /* Write to stderr; 0 = none (default); 1 = errors; 2 = progression; 3 = details; 4 = debug; */
    unsigned dictID;             /* 0 means auto mode (32-bits random value); other : force dictID value */
    unsigned reserved[2];        /* reserved space for future parameters */
} ZDICT_params_t;


/*! ZDICT_trainFromBuffer_advanced() :
    Same as ZDICT_trainFromBuffer() with control over more parameters.
    `parameters` is optional and can be provided with values set to 0 to mean "default".
    @return : size of dictionary stored into `dictBuffer` (<= `dictBufferSize`),
              or an error code, which can be tested by ZDICT_isError().
    note : ZDICT_trainFromBuffer_advanced() will send notifications into stderr if instructed to, using notificationLevel>0.
*/
size_t ZDICT_trainFromBuffer_advanced(void* dictBuffer, size_t dictBufferCapacity,
                                const void* samplesBuffer, const size_t* samplesSizes, unsigned nbSamples,
                                ZDICT_params_t parameters);


/*! ZDICT_addEntropyTablesFromBuffer() :

    Given a content-only dictionary (built using any 3rd party algorithm),
    add entropy tables computed from an array of samples.
    Samples must be stored concatenated in a flat buffer `samplesBuffer`,
    supplied with an array of sizes `samplesSizes`, providing the size of each sample in order.

    The input dictionary content must be stored *at the end* of `dictBuffer`.
    Its size is `dictContentSize`.
    The resulting dictionary with added entropy tables will be *written back to `dictBuffer`*,
    starting from its beginning.
    @return : size of dictionary stored into `dictBuffer` (<= `dictBufferCapacity`).
*/
size_t ZDICT_addEntropyTablesFromBuffer(void* dictBuffer, size_t dictContentSize, size_t dictBufferCapacity,
                                        const void* samplesBuffer, const size_t* samplesSizes, unsigned nbSamples);



#endif   /* ZDICT_STATIC_LINKING_ONLY */

#if defined (__cplusplus)
}
#endif

#endif   /* DICTBUILDER_H_001 */
