---
slug: /ja/faq/operations/multi-region-replication
title: ClickHouseはマルチリージョンレプリケーションをサポートしていますか？
toc_hidden: true
toc_priority: 30
---

# ClickHouseはマルチリージョンレプリケーションをサポートしていますか？ {#does-clickhouse-support-multi-region-replication}

短く言えば「はい」です。ただし、すべてのリージョン/データセンター間の遅延はできるだけ二桁程度に保つことをお勧めします。さもなければ、分散合意プロトコルを経由するため、書き込みパフォーマンスが低下します。たとえば、米国の東西間のレプリケーションは問題なく動作する可能性がありますが、米国とヨーロッパ間ではそうではないかもしれません。

設定に関しては、シングルリージョンのレプリケーションと比べて違いはありません。単に異なる場所にあるホストをレプリカとして使用してください。

詳細については、[データレプリケーションに関する完全な記事](../../engines/table-engines/mergetree-family/replication.md)をご覧ください。
