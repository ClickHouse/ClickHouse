# Minimal MeCab dictionary fixture

`minimal_dic.tar.gz` is a tiny compiled MeCab dictionary used only by this test — not a real
Japanese dictionary. It holds a handful of entries (see `src/`) so the `japanese` tokenizer can be
exercised without a large official dictionary.

`src/` contains the readable dictionary sources: `dict.csv` (lexicon), `matrix.def`, `char.def`,
`unk.def`, `dicrc`.

To regenerate the binary after editing `src/`, run `./generate.sh`. It builds MeCab's
`mecab-dict-index` from the vendored `contrib/MeCab` sources, compiles the dictionary, packs it
reproducibly, and prints the SHA-256. If the SHA changes, update `<dictionary_sha>` in
`../configs/mecab_tokenizer.xml`.
