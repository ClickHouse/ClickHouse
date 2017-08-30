/**
 * Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

/*-*************************************
*  Dependencies
***************************************/
#include "error_private.h"
#include "zbuff.h"

/*-****************************************
*  ZBUFF Error Management  (deprecated)
******************************************/

/*! ZBUFF_isError() :
*   tells if a return value is an error code */
unsigned ZBUFF_isError(size_t errorCode) { return ERR_isError(errorCode); }
/*! ZBUFF_getErrorName() :
*   provides error code string from function result (useful for debugging) */
const char* ZBUFF_getErrorName(size_t errorCode) { return ERR_getErrorName(errorCode); }

