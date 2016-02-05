// Copyright 2009 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#include "util/util.h"
#include "util/valgrind.h"

namespace re2 {

#ifndef __has_feature
#define __has_feature(x) 0
#endif

int RunningOnValgrind() {
#if __has_feature(memory_sanitizer)
	return true;
#elif defined(RUNNING_ON_VALGRIND)
	return RUNNING_ON_VALGRIND;
#else
	return 0;
#endif
}

}  // namespace re2
