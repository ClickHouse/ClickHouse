"""Declare the Mongo C driver headers installed by libbson and libmongoc."""

import json

from initial_migration.compact_build import Source


def declare_mongo_c_headers(path):
    source = Source(path.read_text())
    bson = "src/libbson/src/bson/"
    mongoc = "src/libmongoc/src/mongoc/"
    common = "src/common/src/"
    utf8 = "src/utf8proc-2.11.3/"
    config = ":config/contrib/mongo-c-driver/"
    public = "_MONGOC_PUBLIC_HEADERS"
    # libmongoc's HEADERS install list contains these source headers plus its
    # generated config and version. The two Secure Transport/Channel headers
    # and their implementations are not used by the Linux configuration.
    public_expr = f'glob({json.dumps([mongoc + "mongoc*.h"])}, exclude = {json.dumps([mongoc + "*-private.h", mongoc + "mongoc-stream-tls-secure-*.h"])})'
    disabled = [mongoc + name + ".h" for name in (
        "mcd-integer", "mongoc-cyrus-private", "mongoc-secure-channel-private",
        "mongoc-secure-transport-private", "mongoc-sspi-private",
        "mongoc-stream-tls-secure-channel", "mongoc-stream-tls-secure-transport",
    )]
    def glob(*patterns):
        return f"glob({json.dumps(patterns)})"

    bson_private = glob(bson + "*-private.h", "src/libbson/src/jsonsl/*.h")
    bson_public = f'glob({json.dumps([common + "**/*.h", bson + "*.h"])}, exclude = {json.dumps([common + "mlib/str_vec.h", bson + "*-private.h"])})'
    mongoc_private = f'glob({json.dumps([mongoc + "*.h", utf8 + "*.h", "src/uthash/**/*.h"])}, exclude = {public} + {json.dumps(disabled)})'
    c_sources = glob(common + "*.c")
    attrs = {
        "_libbson": {
            "srcs": " + ".join([glob(bson + "*.c"), '["src/libbson/src/jsonsl/jsonsl.c"]', c_sources, bson_private]),
            "hdrs": json.dumps([config + common + "common-config.h", config + bson + "config.h", config + bson + "version.h"]) + " + " + bson_public,
        },
        "_libmongoc": {
            "srcs": " + ".join([glob(mongoc + "*.c"), c_sources, json.dumps([utf8 + "utf8proc.c", config + mongoc + "mongoc-config-private.h"]), mongoc_private]),
            "hdrs": json.dumps([config + mongoc + "mongoc-config.h", config + mongoc + "mongoc-version.h"]) + " + " + public,
            "private_hdrs": json.dumps([mongoc + "mongoc-counters.defs", utf8 + "utf8proc_data.c"]),
        },
    }
    # A previous import can have any of these glob declarations.
    names = {"_BSON_PRIVATE_HEADERS", "_MONGOC_PRIVATE_HEADERS", public}
    edits = [(*source.span(st), "") for st in source.module.body
             if any(getattr(n, "id", None) in names for n in getattr(st, "targets", []))]
    for st, call in source.rules():
        fields = {kw.arg: source.text(kw.value) for kw in call.keywords}
        name = json.loads(fields["name"])
        if name in attrs:
            fields.pop("textual_hdrs", None)
            fields.pop("additional_compiler_inputs", None)
            fields.update(attrs.pop(name))
            text = source.text(call.func) + "(\n" + "".join(f"    {k} = {v},\n" for k, v in fields.items()) + ")"
            if name == "_libmongoc":
                text = public + " = " + public_expr + "\n\n" + text
            edits.append((*source.span(st), text))
    if attrs:
        raise ValueError(f"Missing Mongo C library: {list(attrs)}")
    path.write_text(source.rewrite(edits))
