// Copyright 2009 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifndef UTIL_UTIL_H_
#define UTIL_UTIL_H_

// TODO(junyer): Get rid of this.
#include <string>
using std::string;

#define arraysize(array) (int)(sizeof(array)/sizeof((array)[0]))

#ifndef FALLTHROUGH_INTENDED
#define FALLTHROUGH_INTENDED do { } while (0)
#endif

#ifndef NO_THREAD_SAFETY_ANALYSIS
#define NO_THREAD_SAFETY_ANALYSIS
#endif

#endif  // UTIL_UTIL_H_
