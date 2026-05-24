//! In-process skim fuzzy finder. Adapted from the previous standalone
//! `rust/workspace/skim` crate; merged here so the cxx bridge runtime is
//! compiled once for the whole line-editor stack.

use skim::prelude::*;
use std::panic;
use term::terminfo::TermInfo;

struct Item {
    text_no_newlines: String,
    orig_text: String,
}

impl Item {
    fn new(text: String) -> Self {
        Self {
            // Text shown / matched against. Newlines collapsed because skim
            // can leave stray glyphs otherwise.
            text_no_newlines: text.replace('\n', " "),
            // Returned verbatim on selection.
            orig_text: text,
        }
    }
}

impl SkimItem for Item {
    fn text(&self) -> Cow<str> {
        Cow::Borrowed(&self.text_no_newlines)
    }

    fn output(&self) -> Cow<str> {
        Cow::Borrowed(&self.orig_text)
    }
}

fn run_impl(prefix: &str, words: &[String]) -> Result<String, String> {
    if let Err(err) = TermInfo::from_env() {
        return Err(format!("{err}"));
    }

    let options = SkimOptionsBuilder::default()
        .height("30%".to_string())
        .query(Some(prefix.to_string()))
        .tac(true)
        // Don't clear on start; do clear on exit. Refs:
        // https://github.com/lotabout/skim/issues/494#issuecomment-1776565846
        .no_clear_start(true)
        .no_clear(false)
        .tiebreak(vec![RankCriteria::NegScore])
        // Exact mode is a better fit for SQL — fuzzy is too forgiving and
        // penalizes case mismatches we don't care about. Splitting on
        // spaces still lets users do "sy qu log" -> "system.query_log".
        .exact(true)
        .case(CaseMatching::Ignore)
        .build()
        .unwrap();

    let (tx, rx): (SkimItemSender, SkimItemReceiver) = unbounded();
    for w in words {
        tx.send(Arc::new(Item::new(w.clone()))).unwrap();
    }
    drop(tx);

    let output = Skim::run_with(&options, Some(rx));
    let output = match output {
        Some(o) => o,
        None => return Err("skim returned nothing".to_string()),
    };
    if output.is_abort {
        return Ok(String::new());
    }
    if !output.selected_items.is_empty() {
        return Ok(output.selected_items[0].output().to_string());
    }
    if !output.query.is_empty() {
        return Ok(output.query);
    }
    Err("No items had been selected".to_string())
}

pub fn run(prefix: &str, words: &[String]) -> Result<String, String> {
    match panic::catch_unwind(|| run_impl(prefix, words)) {
        Err(err) => {
            let e = if let Some(s) = err.downcast_ref::<String>() {
                s.clone()
            } else if let Some(s) = err.downcast_ref::<&str>() {
                (*s).to_string()
            } else {
                format!("Unknown panic type: {:?}", err.type_id())
            };
            Err(format!("Rust panic: {e}"))
        }
        Ok(res) => res,
    }
}
