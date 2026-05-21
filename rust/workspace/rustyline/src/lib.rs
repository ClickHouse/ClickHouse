//! ClickHouse line editor — a thin shim over `rustyline` that delegates
//! highlighting, completion and "open in $EDITOR" back to C++ via cxx.
//!
//! The C++ side is responsible for all SQL-aware behavior (lexing,
//! highlighting, suggestion lookup); this crate only handles terminal IO,
//! history, and key-binding plumbing.

mod fuzzy;
mod history_migrate;

use cxx::CxxString;
use std::borrow::Cow;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};

use rustyline::completion::{Completer, Pair};
use rustyline::config::Configurer;
use rustyline::error::ReadlineError;
use rustyline::highlight::{CmdKind, Highlighter};
use rustyline::hint::Hinter;
use rustyline::history::{FileHistory, History, SearchDirection};
use rustyline::KeyCode;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{
    Cmd, ConditionalEventHandler, Editor as RlEditor, Event, EventContext,
    EventHandler, Helper as RlHelper, KeyEvent, Modifiers, Movement, RepeatCount, Word,
};

#[cxx::bridge(namespace = "DB::rustyline")]
mod ffi {
    /// Status returned from a single `read_line` call.
    #[derive(Debug)]
    enum ReadStatus {
        /// User submitted a line (possibly empty).
        Line,
        /// Ctrl-D on empty buffer / stream closed.
        Eof,
        /// Ctrl-C.
        Interrupted,
        /// Other terminal/IO error (text contains description).
        Error,
    }

    /// Options for constructing the editor.
    struct EditorOptions {
        history_max_entries: u32,
        ignore_shell_suspend: bool,
        embedded_mode: bool,
        interactive_history_legacy_keymap: bool,
        enable_highlight: bool,
        multiline: bool,
        word_break_characters: String,
    }

    /// Result of a `read_line` invocation.
    struct ReadResult {
        status: ReadStatus,
        text: String,
    }

    extern "Rust" {
        type Editor;

        fn new_editor(opts: &EditorOptions) -> Result<Box<Editor>>;
        fn read_line(self: &mut Editor, prompt: &CxxString) -> Result<ReadResult>;
        fn add_history(self: &mut Editor, line: &CxxString) -> Result<()>;
        fn load_history(self: &mut Editor, path: &CxxString) -> Result<()>;
        fn save_history(self: &mut Editor, path: &CxxString) -> Result<()>;
        fn history_lines(self: &Editor) -> Vec<String>;
        fn set_preload(self: &mut Editor, text: &CxxString);
        fn enable_bracketed_paste(self: &mut Editor, on: bool);
        fn set_last_is_delimiter(flag: bool);
    }

    unsafe extern "C++" {
        include!("Client/RustylineCallbacks.h");

        /// Return ANSI-colored rendering of `line`. Display width MUST match
        /// the input — only SGR escapes may be inserted.
        fn cb_highlight(line: &CxxString, pos: usize) -> String;

        /// Byte position at which the to-be-completed token starts.
        fn cb_complete_start(line: &CxxString, pos: usize) -> usize;

        /// Candidate completions for the token at `pos`.
        fn cb_complete_candidates(line: &CxxString, pos: usize) -> Vec<String>;

        /// Open `$EDITOR` with `buf` preloaded. Returns the edited buffer.
        /// `format_query` requests SQL pretty-printing before editing.
        fn cb_open_editor(buf: &CxxString, format_query: bool) -> String;
    }
}

use ffi::{
    cb_complete_candidates, cb_complete_start, cb_highlight, cb_open_editor,
    EditorOptions, ReadResult, ReadStatus,
};

/// Whether the highlighter has determined that the buffer ends in a SQL
/// delimiter (`;` / `\G`). Used by the Ctrl-J / Enter binding to decide
/// between newline and submit.
static LAST_IS_DELIMITER: AtomicBool = AtomicBool::new(false);

fn set_last_is_delimiter(flag: bool) {
    LAST_IS_DELIMITER.store(flag, Ordering::Relaxed);
}

thread_local! {
    /// Snapshot of history entries refreshed at the top of each `read_line`
    /// call. The skim handler reads from here because rustyline's
    /// `EventContext` doesn't expose a `&History`.
    static HISTORY_SNAPSHOT: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

/// The shared helper held inside the rustyline Editor. Owns the configured
/// options; all callbacks reach back into C++.
struct Helper {
    word_break_characters: String,
    enable_highlight: bool,
    /// Last-known cursor position observed in `highlight`, used by
    /// `highlight_char` to refresh on cursor motion.
    last_pos: RefCell<usize>,
}

impl Helper {
    fn new(opts: &EditorOptions) -> Self {
        Self {
            word_break_characters: opts.word_break_characters.clone(),
            enable_highlight: opts.enable_highlight,
            last_pos: RefCell::new(usize::MAX),
        }
    }
}

impl RlHelper for Helper {}

impl Highlighter for Helper {
    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        if !self.enable_highlight {
            return Cow::Borrowed(line);
        }
        *self.last_pos.borrow_mut() = pos;
        cxx::let_cxx_string!(s = line);
        let rendered = cb_highlight(&s, pos);
        Cow::Owned(rendered)
    }

    fn highlight_char(&self, _line: &str, pos: usize, _kind: CmdKind) -> bool {
        if !self.enable_highlight {
            return false;
        }
        // Repaint on every cursor move so the brace-under-cursor brightening
        // (driven by `pos` in `cb_highlight`) stays in sync.
        let mut last = self.last_pos.borrow_mut();
        if *last != pos {
            *last = pos;
            return true;
        }
        false
    }
}

impl Hinter for Helper {
    type Hint = String;
}

impl Completer for Helper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        // Honor word-break characters: rustyline expects us to return the
        // start of the token being completed. C++ already knows our
        // word-break set, so we just hand the full line over and trust the
        // returned `start` position.
        cxx::let_cxx_string!(s = line);
        let start = cb_complete_start(&s, pos);
        let cands = cb_complete_candidates(&s, pos);
        let candidates = cands
            .into_iter()
            .map(|c| Pair {
                display: c.clone(),
                replacement: c,
            })
            .collect();
        Ok((start, candidates))
    }
}

impl Validator for Helper {
    fn validate(&self, ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        // The Enter / Ctrl-J keybindings handle multi-line vs commit
        // directly via LAST_IS_DELIMITER, so the Validator is a no-op.
        let _ = ctx;
        Ok(ValidationResult::Valid(None))
    }
}

/// Handler for Enter / Ctrl-J: insert newline when the buffer is not yet
/// terminated by a SQL delimiter; otherwise submit.
struct CommitOrNewlineHandler;
impl ConditionalEventHandler for CommitOrNewlineHandler {
    fn handle(
        &self,
        _evt: &Event,
        _n: RepeatCount,
        _positive: bool,
        ctx: &EventContext<'_>,
    ) -> Option<Cmd> {
        // Empty buffer -> always submit (replxx behavior).
        if ctx.line().is_empty() {
            return Some(Cmd::AcceptLine);
        }
        if LAST_IS_DELIMITER.load(Ordering::Relaxed) {
            // Reset, then submit.
            LAST_IS_DELIMITER.store(false, Ordering::Relaxed);
            Some(Cmd::AcceptLine)
        } else {
            Some(Cmd::Newline)
        }
    }
}

/// Handler for Ctrl-Z: suppress SIGTSTP by inserting a literal SUB char.
struct SwallowSuspendHandler;
impl ConditionalEventHandler for SwallowSuspendHandler {
    fn handle(
        &self,
        _evt: &Event,
        _n: RepeatCount,
        _positive: bool,
        _ctx: &EventContext<'_>,
    ) -> Option<Cmd> {
        // Insert nothing — eat the keystroke entirely.
        Some(Cmd::Noop)
    }
}

/// Handler for Alt-#: wrap the buffer in a SQL comment and submit.
struct InsertCommentHandler;
impl ConditionalEventHandler for InsertCommentHandler {
    fn handle(
        &self,
        _evt: &Event,
        _n: RepeatCount,
        _positive: bool,
        ctx: &EventContext<'_>,
    ) -> Option<Cmd> {
        let line = ctx.line();
        let commented = if line.contains('\n') {
            // Multi-line: wrap in /* ... */ (nested comments are OK in ClickHouse SQL).
            format!("/* {} */", line)
        } else {
            format!("-- {}", line)
        };
        // Replace whole buffer, then submit. rustyline doesn't expose a
        // "replace buffer then accept" Cmd directly, so we kill-whole-line
        // then insert. The Insert command moves the cursor; we accept after.
        // NOTE: this is approximate — see the plan for the GAP discussion.
        Some(Cmd::Replace(Movement::WholeBuffer, Some(commented)))
    }
}

/// Handler for Ctrl-R / Ctrl-T (skim): fuzzy-search the history snapshot
/// taken at the start of the current `read_line` call.
struct SkimHistoryHandler;
impl ConditionalEventHandler for SkimHistoryHandler {
    fn handle(
        &self,
        _evt: &Event,
        _n: RepeatCount,
        _positive: bool,
        ctx: &EventContext<'_>,
    ) -> Option<Cmd> {
        let prefix = ctx.line().to_string();
        let selection = HISTORY_SNAPSHOT.with(|h| fuzzy::run(&prefix, &h.borrow()));
        match selection {
            Ok(s) if !s.is_empty() => Some(Cmd::Replace(Movement::WholeBuffer, Some(s))),
            _ => None,
        }
    }
}

/// Handler for Alt-E / Alt-F: open external editor with current buffer.
struct OpenEditorHandler {
    format_query: bool,
}
impl ConditionalEventHandler for OpenEditorHandler {
    fn handle(
        &self,
        _evt: &Event,
        _n: RepeatCount,
        _positive: bool,
        ctx: &EventContext<'_>,
    ) -> Option<Cmd> {
        cxx::let_cxx_string!(s = ctx.line());
        let new_buf = cb_open_editor(&s, self.format_query);
        // Replace the buffer with the editor output. The user has to press
        // Enter again to submit — matching the behavior `replxx` had before
        // we wired the implicit submit in `openEditor`.
        Some(Cmd::Replace(Movement::WholeBuffer, Some(new_buf)))
    }
}

/// The opaque type exposed to C++.
pub struct Editor {
    inner: RlEditor<Helper, FileHistory>,
    /// Text to preload at the next `read_line` call.
    preload: Option<String>,
}

fn new_editor(opts: &EditorOptions) -> Result<Box<Editor>, String> {
    let config = rustyline::Config::builder()
        .max_history_size(opts.history_max_entries as usize)
        .map_err(|e| format!("invalid history size: {e}"))?
        .completion_type(rustyline::CompletionType::List)
        .auto_add_history(false)
        .check_cursor_position(true)
        .indent_size(0)
        .bracketed_paste(true)
        .build();

    let helper = Helper::new(opts);
    let mut ed: RlEditor<Helper, FileHistory> =
        RlEditor::with_config(config).map_err(|e| format!("rustyline init: {e}"))?;
    ed.set_helper(Some(helper));

    // -- Keybindings ---------------------------------------------------------
    use KeyEvent as K;
    let m_ctrl = Modifiers::CTRL;
    let m_alt = Modifiers::ALT;

    // Ctrl-N / Ctrl-P -> history navigation (replxx default was completion).
    ed.bind_sequence(Event::KeySeq(vec![K::new('N', m_ctrl)]), EventHandler::Simple(Cmd::NextHistory));
    ed.bind_sequence(Event::KeySeq(vec![K::new('P', m_ctrl)]), EventHandler::Simple(Cmd::PreviousHistory));

    // Ctrl-Z -> swallow (don't suspend).
    if opts.ignore_shell_suspend {
        ed.bind_sequence(
            Event::KeySeq(vec![K::new('Z', m_ctrl)]),
            EventHandler::Conditional(Box::new(SwallowSuspendHandler)),
        );
    }

    // Enter / Ctrl-J -> commit-or-newline driven by LAST_IS_DELIMITER.
    // EventHandler isn't Clone in rustyline 15, so wrap twice.
    if opts.multiline {
        ed.bind_sequence(
            Event::KeySeq(vec![K::new('\r', Modifiers::NONE)]),
            EventHandler::Conditional(Box::new(CommitOrNewlineHandler)),
        );
        ed.bind_sequence(
            Event::KeySeq(vec![K::new('J', m_ctrl)]),
            EventHandler::Conditional(Box::new(CommitOrNewlineHandler)),
        );
    }

    // Alt-N / Alt-P -> completion navigation (replxx alt remap).
    ed.bind_sequence(
        Event::KeySeq(vec![K::new('N', m_alt)]),
        EventHandler::Simple(Cmd::CompleteHint),
    );
    ed.bind_sequence(
        Event::KeySeq(vec![K::new('P', m_alt)]),
        EventHandler::Simple(Cmd::Complete),
    );

    // Alt-Backspace -> kill word backward (readline default; replxx remaps to this).
    ed.bind_sequence(
        Event::KeySeq(vec![K(KeyCode::Backspace, m_alt)]),
        EventHandler::Simple(Cmd::Kill(Movement::BackwardWord(1, Word::Big))),
    );

    // Ctrl-W -> kill to whitespace on the left.
    ed.bind_sequence(
        Event::KeySeq(vec![K::new('W', m_ctrl)]),
        EventHandler::Simple(Cmd::Kill(Movement::BackwardWord(1, Word::Vi))),
    );

    // Alt-E / Alt-F -> open editor (unless embedded).
    if !opts.embedded_mode {
        ed.bind_sequence(
            Event::KeySeq(vec![K::new('E', m_alt)]),
            EventHandler::Conditional(Box::new(OpenEditorHandler { format_query: false })),
        );
        ed.bind_sequence(
            Event::KeySeq(vec![K::new('F', m_alt)]),
            EventHandler::Conditional(Box::new(OpenEditorHandler { format_query: true })),
        );
    }

    // Alt-# -> wrap-in-comment.
    ed.bind_sequence(
        Event::KeySeq(vec![K::new('#', m_alt)]),
        EventHandler::Conditional(Box::new(InsertCommentHandler)),
    );

    // Ctrl-R / Ctrl-T: one runs skim (fuzzy), the other runs rustyline's
    // built-in incremental reverse-search. The legacy keymap swaps them.
    let (fuzzy_key, regular_key) = if opts.interactive_history_legacy_keymap {
        ('T', 'R')
    } else {
        ('R', 'T')
    };
    ed.bind_sequence(
        Event::KeySeq(vec![K::new(fuzzy_key, m_ctrl)]),
        EventHandler::Conditional(Box::new(SkimHistoryHandler)),
    );
    ed.bind_sequence(
        Event::KeySeq(vec![K::new(regular_key, m_ctrl)]),
        EventHandler::Simple(Cmd::ReverseSearchHistory),
    );

    // Insert key: replxx used this to toggle overwrite mode. rustyline 15
    // doesn't expose a toggle as a Cmd variant, so leave unbound for now.

    Ok(Box::new(Editor {
        inner: ed,
        preload: None,
    }))
}

fn map_readline_error(e: ReadlineError) -> ReadResult {
    match e {
        ReadlineError::Interrupted => ReadResult {
            status: ReadStatus::Interrupted,
            text: String::new(),
        },
        ReadlineError::Eof => ReadResult {
            status: ReadStatus::Eof,
            text: String::new(),
        },
        other => ReadResult {
            status: ReadStatus::Error,
            text: format!("{other}"),
        },
    }
}

impl Editor {
    fn read_line(&mut self, prompt: &CxxString) -> Result<ReadResult, String> {
        let prompt_str = prompt.to_str().map_err(|e| format!("prompt utf-8: {e}"))?;
        let snapshot = self.history_lines();
        HISTORY_SNAPSHOT.with(|h| *h.borrow_mut() = snapshot);
        let preload = self.preload.take();
        let res = if let Some(text) = preload {
            self.inner.readline_with_initial(prompt_str, (&text, ""))
        } else {
            self.inner.readline(prompt_str)
        };
        Ok(match res {
            Ok(line) => ReadResult {
                status: ReadStatus::Line,
                text: line,
            },
            Err(e) => map_readline_error(e),
        })
    }

    fn add_history(&mut self, line: &CxxString) -> Result<(), String> {
        let s = line.to_str().map_err(|e| format!("utf-8: {e}"))?;
        self.inner
            .add_history_entry(s)
            .map(|_| ())
            .map_err(|e| format!("history add: {e}"))
    }

    fn load_history(&mut self, path: &CxxString) -> Result<(), String> {
        let p = path.to_str().map_err(|e| format!("utf-8: {e}"))?;
        // One-shot migration: if the file is in the old replxx
        // `### YYYY-…` marker format, rewrite it as rustyline V2 first so
        // the marker lines don't surface as bogus history entries (and
        // pollute Ctrl-R skim results).
        history_migrate::migrate(p)
            .map_err(|e| format!("history migrate: {e}"))?;
        self.inner
            .load_history(p)
            .map_err(|e| format!("load history: {e}"))
    }

    fn save_history(&mut self, path: &CxxString) -> Result<(), String> {
        let p = path.to_str().map_err(|e| format!("utf-8: {e}"))?;
        self.inner
            .save_history(p)
            .map_err(|e| format!("save history: {e}"))
    }

    fn history_lines(&self) -> Vec<String> {
        let hist = self.inner.history();
        let n = hist.len();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            if let Ok(Some(sr)) = hist.get(i, SearchDirection::Forward) {
                out.push(sr.entry.into_owned());
            }
        }
        out
    }

    fn set_preload(&mut self, text: &CxxString) {
        match text.to_str() {
            Ok(s) if !s.is_empty() => self.preload = Some(s.to_string()),
            _ => self.preload = None,
        }
    }

    fn enable_bracketed_paste(&mut self, on: bool) {
        self.inner.enable_bracketed_paste(on);
    }
}
