use cxx::{CxxString, CxxVector};
use skim::prelude::*;
use std::panic;
use term::terminfo::TermInfo;

#[cxx::bridge]
mod ffi {
    extern "Rust" {
        fn skim(prefix: &CxxString, words: &CxxVector<CxxString>) -> Result<String>;
    }
}

struct Item {
    text_no_newlines: String,
    orig_text: String,
}
impl Item {
    fn new(text: String) -> Self {
        Self {
            // Text that will be printed by skim, and will be used for matching.
            //
            // Text that will be shown should not contains new lines since in this case skim may
            // live some symbols on the screen, and this looks odd.
            text_no_newlines: text.replace("\n", " "),
            // This will be used when the match had been selected.
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

fn skim_impl(prefix: &CxxString, words: &CxxVector<CxxString>) -> Result<String, String> {
    // Let's check is terminal available. To avoid panic.
    if let Err(err) = TermInfo::from_env() {
        return Err(format!("{}", err));
    }

    let options = SkimOptionsBuilder::default()
        .height(Some("30%"))
        .query(Some(prefix.to_str().unwrap()))
        .tac(true)
        // Do not clear on start and clear on exit will clear skim output from the terminal.
        //
        // Refs: https://github.com/lotabout/skim/issues/494#issuecomment-1776565846
        .no_clear_start(true)
        .no_clear(false)
        .tiebreak(Some("-score".to_string()))
        // Exact mode performs better for SQL.
        //
        // Default fuzzy search is too smart for SQL, it even takes into account the case, which
        // should not be accounted (you don't want to type "SELECT" instead of "select" to find the
        // query).
        //
        // Exact matching seems better algorithm for SQL, it is not 100% exact, it splits by space,
        // and apply separate matcher actually for each word.
        // Note, that if you think that "space is not enough" as the delimiter, then you should
        // first know that this is the delimiter only for the input query, so to match
        // "system.query_log" you can use "sy qu log"
        // Also it should be more common for users who did not know how to use fuzzy search.
        // (also you can disable exact mode by prepending "'" char).
        //
        // Also it ignores the case correctly, i.e. it does not have penalty for case mismatch,
        // like fuzzy algorithms (take a look at SkimScoreConfig::penalty_case_mismatch).
        .exact(true)
        .case(CaseMatching::Ignore)
        .build()
        .unwrap();

    let (tx, rx): (SkimItemSender, SkimItemReceiver) = unbounded();
    for word in words {
        tx.send(Arc::new(Item::new(word.to_string()))).unwrap();
    }
    // so that skim could know when to stop waiting for more items.
    drop(tx);

    let output = Skim::run_with(&options, Some(rx));
    if output.is_none() {
        return Err("skim return nothing".to_string());
    }
    let output = output.unwrap();
    if output.is_abort {
        return Ok("".to_string());
    }

    if output.selected_items.is_empty() {
        return Err("No items had been selected".to_string());
    }
    Ok(output.selected_items[0].output().to_string())
}

fn skim(prefix: &CxxString, words: &CxxVector<CxxString>) -> Result<String, String> {
    match panic::catch_unwind(|| skim_impl(prefix, words)) {
        Err(err) => {
            let e = if let Some(s) = err.downcast_ref::<String>() {
                format!("{}", s)
            } else if let Some(s) = err.downcast_ref::<&str>() {
                format!("{}", s)
            } else {
                format!("Unknown panic type: {:?}", err.type_id())
            };
            Err(format!("Rust panic: {:?}", e))
        }
        Ok(res) => res,
    }
}
