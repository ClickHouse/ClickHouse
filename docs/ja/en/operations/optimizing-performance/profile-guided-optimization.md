---
description: 'プロファイル誘導最適化に関するドキュメント'
sidebar_label: 'プロファイル誘導最適化（PGO）'
sidebar_position: 54
slug: /operations/optimizing-performance/profile-guided-optimization
title: 'プロファイル誘導最適化'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<div id="profile-guided-optimization">
  # プロファイル誘導最適化
</div>

Profile-Guided Optimization (PGO) は、実行時プロファイルに基づいてプログラムを最適化するコンパイラ最適化手法です。

テストによると、PGO は ClickHouse のパフォーマンス向上に役立ちます。ClickBench のテストスイートでは、QPS が最大 15% 向上することが確認されています。より詳しい結果は[こちら](https://pastebin.com/xbue3HMU)をご覧ください。パフォーマンス向上の度合いは、通常のワークロードによって異なり、より良い結果が得られる場合もあれば、逆に悪化する場合もあります。

ClickHouse における PGO の詳細については、該当する GitHub の[issue](https://github.com/ClickHouse/ClickHouse/issues/44567)をご覧ください。

<div id="how-to-build-clickhouse-with-pgo">
  ## PGO を使用して ClickHouse をビルドするには？
</div>

PGO には主に 2 種類あります。[インストルメンテーション](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers) と [サンプリング](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers) (AutoFDO とも呼ばれます) です。このガイドでは、ClickHouse におけるインストルメンテーション PGO について説明します。

1. ClickHouse をインストルメンテーションモードでビルドします。Clang では、`CXXFLAGS` に `-fprofile-generate` オプションを指定することで実行できます。
2. インストルメンテーションモードでビルドした ClickHouse を、サンプルワークロードで実行します。ここでは、普段使用しているワークロードを使う必要があります。方法の 1 つとして、[ClickBench](https://github.com/ClickHouse/ClickBench) をサンプルワークロードとして使用できます。インストルメンテーションモードの ClickHouse は遅くなる可能性があるため、その点を考慮し、パフォーマンスが重要な環境では実行しないでください。
3. 前の手順で収集したプロファイルと `-fprofile-use` コンパイラフラグを使って、ClickHouse をもう一度コンパイルします。

PGO の適用方法についてさらに詳しくは、Clang の[ドキュメント](https://clang.llvm.org/docs/UsersManual.html#profile-guided-optimization)を参照してください。

サンプルワークロードを production 環境から直接収集する場合は、サンプリング PGO の利用を検討することをおすすめします。