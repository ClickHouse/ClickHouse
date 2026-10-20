"""Check direct, unused and private C++ dependencies of the requested targets."""

load("@depend_on_what_you_use//dwyu/cc:defs.bzl", "dwyu_cc_aspect_factory", _dwyu_cc_consumer_audit = "dwyu_cc_consumer_audit")

dwyu = dwyu_cc_aspect_factory(
    analysis_optimizes_impl_deps = True,
    preprocessing_mode = "compiler",
)

# Dev-only rule: analyze textual headers in actual configured consumers.
dwyu_cc_consumer_audit = _dwyu_cc_consumer_audit
