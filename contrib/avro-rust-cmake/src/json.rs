//! A small JSON parser for Avro schemas.
//!
//! * No recursion: containers are tracked on an explicit stack, so nesting
//!   depth costs heap, not native stack. The depth limit has the fork's
//!   semantics (the root value has depth 0, every container adds one, the
//!   check is `depth > max` on entering any value, `max == 0` means "no
//!   configured limit", which here is the internal [`HARD_MAX_DEPTH`]).
//! * Explicit caps on input bytes and node count.
//! * Nodes live in an arena and refer to children by index. Dropping a document
//!   never recurses.
//! * Strict RFC 8259 grammar: one value, nothing after it, no comments, no
//!   trailing commas, no control characters inside strings. Duplicate object
//!   keys are kept; lookups return the first one (the fork's behaviour).

use crate::{Error, ErrorCode, Result, HARD_MAX_DEPTH};

pub type JsonId = u32;

#[derive(Clone, Copy, Debug)]
pub struct JsonLimits {
    /// Maximum nesting depth; `0` selects [`HARD_MAX_DEPTH`].
    pub max_depth: usize,
    /// Maximum input size in bytes.
    pub max_bytes: usize,
    /// Maximum number of values (scalars and containers) in the document.
    pub max_nodes: usize,
}

/// The default `max_nodes` admits the existing 100,000-field named-record compatibility case (about 1.1
/// million JSON values). `1 << 21` values bound the node arena itself to 64 MiB because a `JsonValue` is 32
/// bytes. Input bytes themselves are not capped; streaming callers own their input buffers.

impl Default for JsonLimits {
    fn default() -> Self {
        JsonLimits {
            max_depth: 0,
            max_bytes: usize::MAX,
            max_nodes: 1 << 21,
        }
    }
}

#[derive(Debug)]
pub enum JsonValue {
    Null,
    Bool(bool),
    /// An integer literal that fits in `i64`.
    Int(i64),
    /// Any other number (fraction, exponent, or an integer beyond `i64`).
    Float(f64),
    Str(Box<str>),
    Array(Vec<JsonId>),
    Object(Vec<(Box<str>, JsonId)>),
}

#[derive(Debug)]
pub struct JsonDoc {
    nodes: Vec<JsonValue>,
}

impl JsonDoc {
    /// The id of the root value.
    pub const ROOT: JsonId = 0;

    pub fn get(&self, id: JsonId) -> &JsonValue {
        &self.nodes[id as usize]
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// First value stored under `key` in the object `id`, if `id` is an object.
    pub fn object_get(&self, id: JsonId, key: &str) -> Option<JsonId> {
        match self.get(id) {
            JsonValue::Object(members) => {
                members.iter().find(|(k, _)| &**k == key).map(|(_, v)| *v)
            }
            _ => None,
        }
    }

    pub fn as_str(&self, id: JsonId) -> Option<&str> {
        match self.get(id) {
            JsonValue::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_int(&self, id: JsonId) -> Option<i64> {
        match self.get(id) {
            JsonValue::Int(v) => Some(*v),
            _ => None,
        }
    }

    /// Parses `input` as a single JSON value.
    pub fn parse(input: &[u8], limits: JsonLimits) -> Result<JsonDoc> {
        if input.len() > limits.max_bytes {
            return Err(Error::new(
                ErrorCode::SchemaTooLarge,
                format!(
                    "JSON input of {} bytes exceeds the limit of {} bytes",
                    input.len(),
                    limits.max_bytes
                ),
            ));
        }
        let max_depth = if limits.max_depth == 0 {
            HARD_MAX_DEPTH
        } else {
            limits.max_depth.min(HARD_MAX_DEPTH)
        };
        let mut p = Parser {
            input,
            pos: 0,
            doc: JsonDoc { nodes: Vec::new() },
            max_nodes: limits.max_nodes,
        };
        p.run(max_depth)?;
        Ok(p.doc)
    }
}

struct Frame {
    id: JsonId,
    is_object: bool,
}

struct Parser<'a> {
    input: &'a [u8],
    pos: usize,
    doc: JsonDoc,
    max_nodes: usize,
}

impl Parser<'_> {
    fn err(&self, what: &str) -> Error {
        Error::new(
            ErrorCode::JsonSyntax,
            format!("JSON parse error at offset {}: {}", self.pos, what),
        )
    }

    fn skip_ws(&mut self) {
        while let Some(&c) = self.input.get(self.pos) {
            if c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn alloc(&mut self, value: JsonValue) -> Result<JsonId> {
        if self.doc.nodes.len() >= self.max_nodes {
            return Err(Error::new(
                ErrorCode::SchemaTooLarge,
                format!("JSON document has more than {} values", self.max_nodes),
            ));
        }
        let id = self.doc.nodes.len() as JsonId;
        self.doc.nodes.push(value);
        Ok(id)
    }

    /// Adds `id` to the container on top of `stack`, under `key` for objects.
    fn link(&mut self, stack: &[Frame], key: &mut Option<Box<str>>, id: JsonId) {
        if let Some(top) = stack.last() {
            match &mut self.doc.nodes[top.id as usize] {
                JsonValue::Object(members) => {
                    // `key` is always set before a value inside an object.
                    members.push((key.take().unwrap_or_default(), id));
                }
                JsonValue::Array(items) => items.push(id),
                _ => {}
            }
        }
    }

    fn parse_key(&mut self) -> Result<Box<str>> {
        self.skip_ws();
        if self.input.get(self.pos) != Some(&b'"') {
            return Err(self.err("expected a string key"));
        }
        let key = self.parse_string()?;
        self.skip_ws();
        if self.input.get(self.pos) != Some(&b':') {
            return Err(self.err("expected ':' after an object key"));
        }
        self.pos += 1;
        Ok(key)
    }

    fn run(&mut self, max_depth: usize) -> Result<()> {
        let mut stack: Vec<Frame> = Vec::new();
        let mut key: Option<Box<str>> = None;
        let mut want_value = true;
        loop {
            if want_value {
                self.skip_ws();
                if stack.len() > max_depth {
                    return Err(Error::new(
                        ErrorCode::JsonDepth,
                        "JSON nesting depth exceeds the limit",
                    ));
                }
                let Some(&c) = self.input.get(self.pos) else {
                    return Err(self.err("unexpected end of input"));
                };
                match c {
                    b'{' | b'[' => {
                        let is_object = c == b'{';
                        let id = self.alloc(if is_object {
                            JsonValue::Object(Vec::new())
                        } else {
                            JsonValue::Array(Vec::new())
                        })?;
                        self.link(&stack, &mut key, id);
                        self.pos += 1;
                        self.skip_ws();
                        let close = if is_object { b'}' } else { b']' };
                        if self.input.get(self.pos) == Some(&close) {
                            self.pos += 1;
                            // An empty container is a complete value. Its children (none)
                            // would have depth + 1, so no depth check is skipped.
                            want_value = false;
                        } else {
                            stack.push(Frame { id, is_object });
                            if is_object {
                                key = Some(self.parse_key()?);
                            }
                        }
                        // For a non-empty container `want_value` stays true: its first
                        // child follows.
                    }
                    b'"' => {
                        let s = self.parse_string()?;
                        let id = self.alloc(JsonValue::Str(s))?;
                        self.link(&stack, &mut key, id);
                        want_value = false;
                    }
                    b'-' | b'0'..=b'9' => {
                        let v = self.parse_number()?;
                        let id = self.alloc(v)?;
                        self.link(&stack, &mut key, id);
                        want_value = false;
                    }
                    b't' | b'f' | b'n' => {
                        let v = self.parse_literal()?;
                        let id = self.alloc(v)?;
                        self.link(&stack, &mut key, id);
                        want_value = false;
                    }
                    _ => return Err(self.err("unexpected character")),
                }
            } else {
                let Some(top) = stack.last() else {
                    break;
                };
                self.skip_ws();
                match self.input.get(self.pos) {
                    Some(b',') => {
                        self.pos += 1;
                        if top.is_object {
                            key = Some(self.parse_key()?);
                        }
                        want_value = true;
                    }
                    Some(b'}') if top.is_object => {
                        self.pos += 1;
                        stack.pop();
                    }
                    Some(b']') if !top.is_object => {
                        self.pos += 1;
                        stack.pop();
                    }
                    Some(_) => return Err(self.err("expected ',' or the end of the container")),
                    None => return Err(self.err("unexpected end of input")),
                }
            }
        }
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(self.err("unexpected content after the JSON value"));
        }
        Ok(())
    }

    fn parse_literal(&mut self) -> Result<JsonValue> {
        let rest = &self.input[self.pos..];
        let (len, v) = if rest.starts_with(b"true") {
            (4, JsonValue::Bool(true))
        } else if rest.starts_with(b"false") {
            (5, JsonValue::Bool(false))
        } else if rest.starts_with(b"null") {
            (4, JsonValue::Null)
        } else {
            return Err(self.err("invalid literal"));
        };
        if rest.get(len).is_some_and(|c| c.is_ascii_alphanumeric()) {
            return Err(self.err("invalid literal"));
        }
        self.pos += len;
        Ok(v)
    }

    fn parse_number(&mut self) -> Result<JsonValue> {
        let start = self.pos;
        let mut p = self.pos;
        let b = self.input;
        if b.get(p) == Some(&b'-') {
            p += 1;
        }
        match b.get(p) {
            Some(b'0') => {
                p += 1;
                if b.get(p).is_some_and(|c| c.is_ascii_digit()) {
                    self.pos = p;
                    return Err(self.err("leading zeros are not allowed"));
                }
            }
            Some(b'1'..=b'9') => {
                while b.get(p).is_some_and(|c| c.is_ascii_digit()) {
                    p += 1;
                }
            }
            _ => {
                self.pos = p;
                return Err(self.err("invalid number"));
            }
        }
        let mut is_float = false;
        if b.get(p) == Some(&b'.') {
            is_float = true;
            p += 1;
            let digits = p;
            while b.get(p).is_some_and(|c| c.is_ascii_digit()) {
                p += 1;
            }
            if p == digits {
                self.pos = p;
                return Err(self.err("invalid number"));
            }
        }
        if matches!(b.get(p), Some(b'e' | b'E')) {
            is_float = true;
            p += 1;
            if matches!(b.get(p), Some(b'+' | b'-')) {
                p += 1;
            }
            let digits = p;
            while b.get(p).is_some_and(|c| c.is_ascii_digit()) {
                p += 1;
            }
            if p == digits {
                self.pos = p;
                return Err(self.err("invalid number"));
            }
        }
        self.pos = p;
        // The slice is ASCII digits and punctuation by construction.
        let text = std::str::from_utf8(&b[start..p]).map_err(|_| self.err("invalid number"))?;
        if !is_float {
            if let Ok(v) = text.parse::<i64>() {
                return Ok(JsonValue::Int(v));
            }
        }
        match text.parse::<f64>() {
            Ok(v) => Ok(JsonValue::Float(v)),
            Err(_) => Err(self.err("invalid number")),
        }
    }

    fn parse_hex4(&mut self) -> Result<u32> {
        let mut v: u32 = 0;
        for _ in 0..4 {
            let Some(&c) = self.input.get(self.pos) else {
                return Err(self.err("unexpected end of input in a \\u escape"));
            };
            let d = match c {
                b'0'..=b'9' => c - b'0',
                b'a'..=b'f' => c - b'a' + 10,
                b'A'..=b'F' => c - b'A' + 10,
                _ => return Err(self.err("invalid \\u escape")),
            };
            v = v * 16 + u32::from(d);
            self.pos += 1;
        }
        Ok(v)
    }

    /// Parses a string starting at the opening quote.
    fn parse_string(&mut self) -> Result<Box<str>> {
        self.pos += 1;
        let mut out: Vec<u8> = Vec::new();
        loop {
            // Copy the run of ordinary bytes in one go.
            let run_start = self.pos;
            while let Some(&c) = self.input.get(self.pos) {
                if c == b'"' || c == b'\\' || c < 0x20 {
                    break;
                }
                self.pos += 1;
            }
            out.extend_from_slice(&self.input[run_start..self.pos]);
            let Some(&c) = self.input.get(self.pos) else {
                return Err(self.err("unterminated string"));
            };
            match c {
                b'"' => {
                    self.pos += 1;
                    break;
                }
                b'\\' => {
                    self.pos += 1;
                    let Some(&e) = self.input.get(self.pos) else {
                        return Err(self.err("unterminated string"));
                    };
                    self.pos += 1;
                    match e {
                        b'"' => out.push(b'"'),
                        b'\\' => out.push(b'\\'),
                        b'/' => out.push(b'/'),
                        b'b' => out.push(8),
                        b'f' => out.push(12),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'u' => {
                            let hi = self.parse_hex4()?;
                            let cp = if (0xD800..0xDC00).contains(&hi) {
                                if self.input.get(self.pos) != Some(&b'\\')
                                    || self.input.get(self.pos + 1) != Some(&b'u')
                                {
                                    return Err(self.err("unpaired surrogate in a \\u escape"));
                                }
                                self.pos += 2;
                                let lo = self.parse_hex4()?;
                                if !(0xDC00..0xE000).contains(&lo) {
                                    return Err(self.err("unpaired surrogate in a \\u escape"));
                                }
                                0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
                            } else if (0xDC00..0xE000).contains(&hi) {
                                return Err(self.err("unpaired surrogate in a \\u escape"));
                            } else {
                                hi
                            };
                            let ch =
                                char::from_u32(cp).ok_or_else(|| self.err("invalid code point"))?;
                            let mut buf = [0u8; 4];
                            out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                        }
                        _ => return Err(self.err("invalid escape")),
                    }
                }
                _ => return Err(self.err("control character in a string")),
            }
        }
        match String::from_utf8(out) {
            Ok(s) => Ok(s.into_boxed_str()),
            Err(_) => Err(self.err("invalid UTF-8 in a string")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<JsonDoc> {
        JsonDoc::parse(s.as_bytes(), JsonLimits::default())
    }

    fn parse_depth(s: &str, max_depth: usize) -> Result<JsonDoc> {
        JsonDoc::parse(
            s.as_bytes(),
            JsonLimits {
                max_depth,
                ..JsonLimits::default()
            },
        )
    }

    #[test]
    fn scalars() {
        assert!(matches!(parse("null").unwrap().get(0), JsonValue::Null));
        assert!(matches!(
            parse(" true ").unwrap().get(0),
            JsonValue::Bool(true)
        ));
        assert!(matches!(parse("-12").unwrap().get(0), JsonValue::Int(-12)));
        assert!(matches!(parse("1e2").unwrap().get(0), JsonValue::Float(v) if *v == 100.0));
        assert!(matches!(
            parse("9223372036854775808").unwrap().get(0),
            JsonValue::Float(_)
        ));
        assert!(
            matches!(parse("\"a\\u00e9\\ud83d\\ude00\\n\"").unwrap().get(0), JsonValue::Str(s) if &**s == "a\u{e9}\u{1F600}\n")
        );
    }

    #[test]
    fn rejects_invalid() {
        for bad in [
            "",
            " ",
            "tru",
            "nul",
            "truee",
            "01",
            "1.",
            ".5",
            "-",
            "+1",
            "1e",
            "[1,]",
            "[,1]",
            "{\"a\":1,}",
            "{\"a\"}",
            "{a:1}",
            "[1 2]",
            "\"abc",
            "\"\\x\"",
            "\"\\ud800\"",
            "\"\\udc00\"",
            "\"\\ud800\\u0041\"",
            "\"a\nb\"",
            "[]]",
            "{} x",
            "NaN",
            "Infinity",
            "'a'",
            "[1}",
            "{\"a\":1]",
        ] {
            assert!(parse(bad).is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn duplicate_keys_first_wins() {
        let d = parse("{\"a\":1,\"a\":2}").unwrap();
        let v = d.object_get(0, "a").unwrap();
        assert_eq!(d.as_int(v), Some(1));
    }

    #[test]
    fn depth_semantics_match_the_fork() {
        // Root has depth 0, so `[[1]]` has the number at depth 2.
        assert!(parse_depth("[[1]]", 2).is_ok());
        let e = parse_depth("[[1]]", 1).unwrap_err();
        assert_eq!(e.message, "JSON nesting depth exceeds the limit");
        assert_eq!(e.code, ErrorCode::JsonDepth);
        // An empty container at depth `max` is fine; its (absent) children are not checked.
        assert!(parse_depth("[[]]", 1).is_ok());
        assert!(parse_depth("[]", 0).is_ok());
        // `0` is "unlimited" up to the hard cap.
        let deep = "[".repeat(50_000) + &"]".repeat(50_000);
        assert!(parse_depth(&deep, 0).is_ok());
        let too_deep = "[".repeat(HARD_MAX_DEPTH + 2) + &"]".repeat(HARD_MAX_DEPTH + 2);
        assert_eq!(
            parse_depth(&too_deep, 0).unwrap_err().code,
            ErrorCode::JsonDepth
        );
    }

    #[test]
    fn caps() {
        let limits = JsonLimits {
            max_nodes: 3,
            ..JsonLimits::default()
        };
        assert!(JsonDoc::parse(b"[1,2]", limits).is_ok());
        assert_eq!(
            JsonDoc::parse(b"[1,2,3]", limits).unwrap_err().code,
            ErrorCode::SchemaTooLarge
        );
        let limits = JsonLimits {
            max_bytes: 4,
            ..JsonLimits::default()
        };
        assert_eq!(
            JsonDoc::parse(b"[1,2,3]", limits).unwrap_err().code,
            ErrorCode::SchemaTooLarge
        );
    }
}
