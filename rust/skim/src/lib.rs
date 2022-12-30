use skim::prelude::*;
use term::terminfo::TermInfo;
use cxx::{CxxString, CxxVector};

#[cxx::bridge]
mod ffi {
    extern "Rust" {
        fn skim(prefix: &CxxString, words: &CxxVector<CxxString>) -> Result<String>;
    }
}

struct Item {
    text: String,
}
impl SkimItem for Item {
    fn text(&self) -> Cow<str> {
        return Cow::Borrowed(&self.text);
    }
}

fn skim(prefix: &CxxString, words: &CxxVector<CxxString>) -> Result<String, String> {
    // Let's check is terminal available. To avoid panic.
    if let Err(err) = TermInfo::from_env() {
        return Err(format!("{}", err));
    }

    let options = SkimOptionsBuilder::default()
        .height(Some("30%"))
        .query(Some(prefix.to_str().unwrap()))
        .tac(true)
        .tiebreak(Some("-score".to_string()))
        .build()
        .unwrap();

    let (tx, rx): (SkimItemSender, SkimItemReceiver) = unbounded();
    for word in words {
        tx.send(Arc::new(Item{ text: word.to_string() })).unwrap();
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
    return Ok(output.selected_items[0].output().to_string());
}
