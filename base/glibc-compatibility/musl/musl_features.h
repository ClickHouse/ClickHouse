#pragma once

#define weak __attribute__((__weak__))
#define hidden __attribute__((__visibility__("hidden")))
#define predict_false(x) __builtin_expect(x, 0)
