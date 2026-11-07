//! Parsing Canonical Form (Apache Avro specification 1.12, "Parsing Canonical
//! Form for Schemas") and the CRC-64-AVRO fingerprint.
//!
//! The transformation is a depth-first walk of the schema graph with an
//! explicit work stack:
//!
//! * `[PRIMITIVES]` a primitive is just its name (`"int"`);
//! * `[FULLNAMES]` named types print their full name, and a named type that was
//!   already printed is printed as its name only (that is what makes recursion
//!   finite);
//! * `[STRIP]` only `type`, `name`, `fields`, `symbols`, `items`, `values`,
//!   `size` survive, so logical types, defaults, docs and aliases are gone;
//! * `[ORDER]` attributes appear in the order `name`, `type`, `fields`,
//!   `symbols`, `items`, `values`, `size`;
//! * `[STRINGS]` strings carry no `\u` escapes: the characters themselves in
//!   UTF-8, with only `"`, `\` and control characters escaped, as JSON needs;
//! * `[INTEGERS]` `size` is a bare integer;
//! * `[WHITESPACE]` none.

use std::fmt::Write;

use crate::schema::{Kind, NodeId, Schema};

enum Tok {
    Node(NodeId),
    Lit(&'static str),
    /// A JSON string literal for a name or symbol.
    Str(String),
}

fn push_quoted(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => {
                // Writing to a `String` cannot fail.
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Cuts `s` to at most `limit` bytes at a character boundary; whether anything was cut.
fn cut(s: &mut String, limit: usize) -> bool {
    if s.len() <= limit {
        return false;
    }
    let mut end = limit;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s.truncate(end);
    true
}

/// `s` cut to at most `limit` bytes at a character boundary, and whether anything was cut.
pub fn truncate_utf8(s: &str, limit: usize) -> (String, bool) {
    let mut out = s.to_string();
    let cut = cut(&mut out, limit);
    (out, cut)
}

/// Parsing Canonical Form of `schema`, generated only while it is no longer than `limit` bytes (plus the text of
/// the node that crossed it): the result is cut to `limit` bytes at a character boundary, and the flag says
/// whether it was cut. With `usize::MAX` the form is complete.
///
/// The form is not bounded by the size of the schema: a named type that is referenced `n` times is printed as
/// its full name `n` times.
pub fn canonical_form(schema: &Schema, limit: usize) -> (String, bool) {
    let mut out = String::new();
    let mut defined = vec![false; schema.node_count()];
    let mut stack: Vec<Tok> = vec![Tok::Node(schema.root())];
    while let Some(tok) = stack.pop() {
        if out.len() > limit {
            cut(&mut out, limit);
            return (out, true);
        }
        match tok {
            Tok::Lit(s) => out.push_str(s),
            Tok::Str(s) => push_quoted(&mut out, &s),
            Tok::Node(id) => {
                let node = schema.node(id);
                match node.kind {
                    Kind::Null
                    | Kind::Boolean
                    | Kind::Int
                    | Kind::Long
                    | Kind::Float
                    | Kind::Double
                    | Kind::Bytes
                    | Kind::String => {
                        out.push('"');
                        out.push_str(node.kind.name());
                        out.push('"');
                    }
                    Kind::Record | Kind::Enum | Kind::Fixed => {
                        let name = node.full_name().unwrap_or("");
                        if defined[id as usize] {
                            push_quoted(&mut out, name);
                            continue;
                        }
                        defined[id as usize] = true;
                        out.push_str("{\"name\":");
                        push_quoted(&mut out, name);
                        match node.kind {
                            Kind::Record => {
                                out.push_str(",\"type\":\"record\",\"fields\":[");
                                // Pushed in reverse: the stack pops the first field first.
                                stack.push(Tok::Lit("]}"));
                                let fields = schema.fields(id);
                                for (i, f) in fields.iter().enumerate().rev() {
                                    stack.push(Tok::Lit("}"));
                                    stack.push(Tok::Node(f.ty));
                                    stack.push(Tok::Lit(",\"type\":"));
                                    stack.push(Tok::Str(f.name.to_string()));
                                    stack.push(Tok::Lit(if i == 0 {
                                        "{\"name\":"
                                    } else {
                                        ",{\"name\":"
                                    }));
                                }
                            }
                            Kind::Enum => {
                                out.push_str(",\"type\":\"enum\",\"symbols\":[");
                                for (i, s) in schema.symbols(id).iter().enumerate() {
                                    if i > 0 {
                                        out.push(',');
                                    }
                                    push_quoted(&mut out, s);
                                }
                                out.push_str("]}");
                            }
                            _ => {
                                let _ = write!(
                                    out,
                                    ",\"type\":\"fixed\",\"size\":{}}}",
                                    node.fixed_size
                                );
                            }
                        }
                    }
                    Kind::Array | Kind::Map => {
                        let (head, items) = if node.kind == Kind::Array {
                            ("{\"type\":\"array\",\"items\":", schema.element(id))
                        } else {
                            ("{\"type\":\"map\",\"values\":", schema.element(id))
                        };
                        out.push_str(head);
                        stack.push(Tok::Lit("}"));
                        if let Some(e) = items {
                            stack.push(Tok::Node(e));
                        }
                    }
                    Kind::Union => {
                        out.push('[');
                        stack.push(Tok::Lit("]"));
                        for (i, &b) in schema.branches(id).iter().enumerate().rev() {
                            stack.push(Tok::Node(b));
                            if i > 0 {
                                stack.push(Tok::Lit(","));
                            }
                        }
                    }
                }
            }
        }
    }
    let truncated = cut(&mut out, limit);
    (out, truncated)
}

const FP_EMPTY: u64 = 0xc15d_213a_a4d7_a795;

fn fp_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    for (i, slot) in table.iter_mut().enumerate() {
        let mut fp = i as u64;
        for _ in 0..8 {
            fp = (fp >> 1) ^ (FP_EMPTY & (fp & 1).wrapping_neg());
        }
        *slot = fp;
    }
    table
}

/// The 64-bit CRC-64-AVRO fingerprint of `bytes` (normally a canonical form).
///
/// Never use this as a cache key on its own: it is trivially forgeable. An identity needs the whole text of
/// the schema ([`Schema::source`]); a hash may only pick the bucket.
pub fn fingerprint64(bytes: &[u8]) -> u64 {
    let table = fp_table();
    let mut fp = FP_EMPTY;
    for &b in bytes {
        fp = (fp >> 8) ^ table[((fp ^ u64::from(b)) & 0xff) as usize];
    }
    fp
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canon(s: &str) -> String {
        Schema::parse(s.as_bytes(), 0)
            .unwrap()
            .canonical_form()
            .to_string()
    }

    #[test]
    fn primitives_and_strip() {
        assert_eq!(canon(r#"{"type":"int"}"#), r#""int""#);
        assert_eq!(
            canon(r#"{"type":"string","logicalType":"uuid","doc":"x"}"#),
            r#""string""#
        );
        assert_eq!(
            canon(r#"{"type":"bytes","logicalType":"decimal","precision":4}"#),
            r#""bytes""#
        );
    }

    #[test]
    fn record_enum_fixed_array_map_union() {
        assert_eq!(
            canon(
                r#"{ "type":"record", "doc":"d", "namespace":"n", "aliases":["z"], "name":"R", "fields":[
                    {"default":3,"name":"a","type":"int","doc":"q","order":"ignore"},
                    {"name":"b","type":{"type":"array","items":{"type":"map","values":["null","long"]}}},
                    {"name":"c","type":{"size":4,"name":"F","type":"fixed"}},
                    {"name":"d","type":{"symbols":["X","Y"],"type":"enum","name":"E"}}]}"#
            ),
            r#"{"name":"n.R","type":"record","fields":[{"name":"a","type":"int"},{"name":"b","type":{"type":"array","items":{"type":"map","values":["null","long"]}}},{"name":"c","type":{"name":"n.F","type":"fixed","size":4}},{"name":"d","type":{"name":"n.E","type":"enum","symbols":["X","Y"]}}]}"#
        );
    }

    #[test]
    fn references_print_names_only() {
        assert_eq!(
            canon(
                r#"{"type":"record","name":"L","fields":[{"name":"v","type":"long"},{"name":"next","type":["null","L"]},{"name":"k","type":{"type":"fixed","name":"K","size":1}},{"name":"k2","type":"K"}]}"#
            ),
            r#"{"name":"L","type":"record","fields":[{"name":"v","type":"long"},{"name":"next","type":["null","L"]},{"name":"k","type":{"name":"K","type":"fixed","size":1}},{"name":"k2","type":"K"}]}"#
        );
    }

    #[test]
    fn namespaces_resolve_to_full_names() {
        // Spec example: a nested type without a namespace inherits the enclosing one,
        // a dotted name ignores it, and an empty `namespace` selects the null namespace.
        assert_eq!(
            canon(
                r#"{"type":"record","name":"X","namespace":"a.b","fields":[
                    {"name":"f1","type":{"type":"enum","name":"Y","symbols":["A"]}},
                    {"name":"f2","type":{"type":"fixed","name":"c.Z","size":1}},
                    {"name":"f3","type":{"type":"fixed","name":"W","namespace":"","size":1}},
                    {"name":"f4","type":"a.b.Y"},
                    {"name":"f5","type":"W"}]}"#
            ),
            r#"{"name":"a.b.X","type":"record","fields":[{"name":"f1","type":{"name":"a.b.Y","type":"enum","symbols":["A"]}},{"name":"f2","type":{"name":"c.Z","type":"fixed","size":1}},{"name":"f3","type":{"name":"W","type":"fixed","size":1}},{"name":"f4","type":"a.b.Y"},{"name":"f5","type":"W"}]}"#
        );
    }

    #[test]
    fn strings_are_unescaped() {
        // `\u00e9` becomes the character itself, quotes and backslashes stay escaped.
        assert_eq!(canon(r#"{"type":"enum","name":"E","symbols":["\u00e9","a\"b","c\\d","\u0001"]}"#), "{\"name\":\"E\",\"type\":\"enum\",\"symbols\":[\"\u{e9}\",\"a\\\"b\",\"c\\\\d\",\"\\u0001\"]}");
    }

    #[test]
    fn crc64_avro_known_value() {
        // The Avro specification and every reference implementation agree that the
        // fingerprint of the canonical form `"null"` is 0x63dd24e7cc258f8a.
        assert_eq!(fingerprint64(br#""null""#), 0x63dd_24e7_cc25_8f8a);
        // The fingerprint of the empty input is the seed.
        assert_eq!(fingerprint64(b""), 0xc15d_213a_a4d7_a795);
    }

    #[test]
    fn deep_schemas_do_not_recurse() {
        let mut s = String::from("\"int\"");
        for _ in 0..30_000 {
            s = format!("{{\"type\":\"array\",\"items\":{s}}}");
        }
        let c = canon(&s);
        assert_eq!(c.len(), s.len());
        assert_eq!(c, s);
    }

    #[test]
    fn prefix_is_a_prefix_of_the_form_and_cut_at_a_character() {
        let text = r#"{"type":"record","name":"R","fields":[{"name":"é","type":{"type":"fixed","name":"F","size":1}},{"name":"b","type":"F"}]}"#;
        let s = Schema::parse(text.as_bytes(), 0).unwrap();
        let (prefix, cut) = s.canonical_prefix(usize::MAX);
        assert!(!cut);
        let full = prefix;
        for limit in 0..=full.len() + 2 {
            let (p, cut) = s.canonical_prefix(limit);
            assert!(full.starts_with(&p));
            assert_eq!(cut, limit < full.len(), "limit {limit}");
            assert!(p.len() <= limit);
            // Cut at a boundary of a character: only a multi-byte character may make it shorter.
            assert!(limit >= full.len() || p.len() + 3 > limit, "limit {limit}");
        }
        // After the form was computed the prefix is a cut of the retained form.
        assert_eq!(s.canonical_form(), full);
        assert_eq!(s.canonical_prefix(10), (full[..10].to_string(), true));
    }
}
