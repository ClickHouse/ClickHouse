# PromQL Grammar

An ANTLR4 grammar for Prometheus Query Language (PromQL).

[PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/)

The grammar was copied from repository antlr/grammars-v4:
https://github.com/antlr/grammars-v4/tree/master/promql

The C++ sources in `generated/` are produced from the grammar by `generate.sh`. Run it after changing the grammar.
