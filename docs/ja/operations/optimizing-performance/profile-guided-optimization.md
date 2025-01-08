---
sidebar_position: 54
sidebar_label: プロファイルガイド最適化 (PGO)
---
import SelfManaged from '@site/docs/ja/_snippets/_self_managed_only_no_roadmap.md';

# プロファイルガイド最適化

プロファイルガイド最適化（PGO）は、プログラムの実行時プロファイルに基づいて最適化を行うコンパイラの最適化技術です。

テストによれば、PGOはClickHouseのパフォーマンス向上に役立ちます。テストによると、ClickBenchテストスイートでQPSが最大15%向上します。より詳細な結果は[こちら](https://pastebin.com/xbue3HMU)にあります。パフォーマンスの利点は、通常のワークロードに依存しますので、結果は良くも悪くもなります。

ClickHouseにおけるPGOの詳細については、関連するGitHubの[課題](https://github.com/ClickHouse/ClickHouse/issues/44567)をご覧ください。

## PGOを使用してClickHouseをビルドするには？

PGOには主に2つの種類があります：[インストルメンテーション](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers)と[サンプリング](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers)（AutoFDOとも呼ばれます）。このガイドでは、ClickHouseにおけるインストルメンテーションPGOについて説明します。

1. インストルメントモードでClickHouseをビルドします。Clangでは、`CXXFLAGS`に`-fprofile-generate`オプションを渡すことで実行できます。
2. サンプルワークロードでインストルメントされたClickHouseを実行します。ここでは、通常のワークロードを使用する必要があります。アプローチの一つとして、サンプルワークロードとして[ClickBench](https://github.com/ClickHouse/ClickBench)を使用することが考えられます。インストルメンテーションモードのClickHouseは動作が遅いかもしれないので、それに備えて、パフォーマンスが重要な環境では実行しないでください。
3. 前のステップで収集したプロファイルを使用して、`-fprofile-use`コンパイラフラグを用いて再度ClickHouseをコンパイルします。

PGOの適用方法についての詳細なガイドは、Clangの[ドキュメント](https://clang.llvm.org/docs/UsersManual.html#profile-guided-optimization)にあります。

生産環境から直接サンプルワークロードを収集する場合は、サンプリングPGOの使用を試みることをお勧めします。
