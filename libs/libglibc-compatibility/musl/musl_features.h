#pragma once

#define weak __attribute__((__weak__))
#define hidden __attribute__((__visibility__("hidden")))
#define weak_alias(old, new) \
        extern __typeof(old) new __attribute__((__weak__, __alias__(#old)))

#define predict_false(x) __builtin_expect(x, 0)
