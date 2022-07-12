This system does not work at all:

```
locustdb> SELECT * FROM default LIMIT 1
thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
thread '<unnamed>' panicked at 'index out of bounds: the len is 65536 but the index is 65536', src/stringpack.rs:91:15
```

It is memory-safe and blazing fast.
