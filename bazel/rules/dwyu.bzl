"""Check direct, unused and private C++ dependencies of the requested targets."""

load("@depend_on_what_you_use//dwyu/cc:defs.bzl", "dwyu_cc_aspect_factory")

dwyu = dwyu_cc_aspect_factory(analysis_optimizes_impl_deps = True)
