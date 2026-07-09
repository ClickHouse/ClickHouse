---
description: 'ClickHouseのバックポートポリシーと自動化の概要'
sidebar_label: 'バックポートシステム'
sidebar_position: 56
slug: /development/backports
title: 'バックポートシステム'
doc_type: 'reference'
---

このドキュメントでは、ClickHouseのバックポートポリシーと、それを実装する自動化システムの概要を説明します。

<div id="release-model">
  ## リリースモデル
</div>

ClickHouse のバージョンは `YY.M.patch.build-type` という形式に従います。ここで、`YY` は西暦の下 2 桁、`M` はリリース月 (先頭の 0 なし) 、`patch` はそのブランチ内のパッチ番号、`build` は単調に増加するビルド番号、`type` は `stable` または `lts` です。

例: `25.3.8.23-lts` — 2025 年 3 月の LTS、パッチ 8、ビルド 23。

リリーストラックは 2 つあります。

* **Stable** リリースは、おおむね毎月公開されます。直近 3 つの stable リリースにパッチが提供されるため、各リリースのアクティブサポート期間は約 3 か月です。
* **LTS (Long-Term Support)** リリースは、毎年 3 月と 8 月に公開されます。2 つの LTS バージョンが同時にサポートされ、それぞれ少なくとも 12 か月間サポートされます。

本番ワークロードを運用しているユーザーには、最新の stable または LTS リリースを使用し、新しいパッチバージョンへ速やかにアップグレードすることを推奨します。パッチリリースに互換性のない変更が含まれることはありません。

<div id="backport-policy">
  ## バックポートポリシー
</div>

すべての変更がバックポートされるわけではありません。リリースブランチの安定性を維持することが目的であるため、バックポートの対象範囲は意図的に限定されています。

* **セキュリティ修正** — 常にバックポートされます。
* **重大なバグ修正** (Exception (論理エラー) 、データ損失、誤った結果、RBAC の問題) — 一般的なバックポートルールに従って自動的にバックポート対象に選ばれます。これは `pr-critical-bugfix` ラベルで識別され、このラベルが付くと `pr-must-backport` が自動的に追加されます。
* **安定性およびリグレッションの修正** — 変更によるリスクが、そのバグを残すリスクに比べて低い場合にバックポートされます。これは、メンテナーが手動で追加する `pr-must-backport` によって識別されます。
* **回避策がある軽微なバグ修正** — リリースブランチの不安定化を避けるため、通常はバックポートされません。
* **新機能、改善、パフォーマンス関連の作業** — バックポートされません。

`pr-must-backport` ラベルは、メンテナーが PR をバックポート対象として指定するための手動オーバーライドです。`pr-critical-bugfix` ラベルが付くと、CI hook によって `pr-must-backport` が自動的に追加されます (`pr_labels_and_category.py` を参照) 。

**競合のエスカレーション。** 自動バックポートでマージ競合を解消できない場合でも、`cherry-pick PR` は必ず作成し、元の PR の著者、マージ実行者、既存の assignees に割り当てる必要があります。これにより、人手で競合を解決してバックポートを完了できます。

<div id="backport-tool">
  ## Backport Tool
</div>

上記で説明したバックポートポリシーは、`tests/ci/cherry_pick.py` の自動化ツールに実装されています。このツールは ClickHouse のインフラストラクチャ上で GitHub Actions の ワークフロー として実行され、アクティブなリリースブランチの検出、バックポート対象となる PR の選定、2 段階の cherry-pick とバックポート手順の実行、競合の管理、遅延ポリシーの適用、ラベルの同期維持など、必要な要件をすべて満たしています。

長期的な目標は、この実装を他のプロジェクトでも採用できるスタンドアロンのオープンソース Python ツールとして切り出すことです。想定している設計は次のとおりです。

* **設定可能** — すべてのポリシーパラメーター (適格なラベル、遅延ウィンドウ、古くなった PR のしきい値、ロールアウト中の挙動など) を設定ファイルで表現し、コードを変更せずに任意のプロジェクトのバックポート要件に合わせてツールを適応できるようにします。
* **配布可能** — ClickHouse の CI インフラストラクチャに依存せず、PyPI からインストール可能な自己完結型の Python wheel としてパッケージ化します。
* **プログラム可能** — プルリクエスト、ラベル、リリースブランチを表す明確なオブジェクトモデルを公開し、利用者がコアエンジンの上に独自の ワークフロー をスクリプトで構築できるようにします。

<div id="testing">
  ### テスト
</div>

スタンドアロンツールの計画に含まれている要素の 1 つに、専用のテストスイートと、それに対応する軽量なテスト用インフラストラクチャがあります。このインフラストラクチャでは、あらかじめ次の内容を用意した一時的な GitHub リポジトリ (またはそれに相当するローカル環境) を立ち上げられるようになります。

* リリースラインを表す、設定可能なブランチ一式
* さまざまな組み合わせのバックポートラベルが付いたプルリクエスト
* リリースブランチを指す `release` ラベルが付いた release PR

これにより、本番環境の状態に影響を与えることなく、実在するものの破棄可能なリポジトリを使って、ラベル検出、cherry-pick ブランチの作成、競合処理、バックポート PR の作成、担当者の割り当てロジック、`rolling-out` のスキップ、遅延ポリシーといった自動化ループ全体をテストできます。同じインフラストラクチャは、ポリシー変更をデプロイする前の回帰テストにも再利用できます。

<div id="active-release-branches">
  ## アクティブなリリースブランチ
</div>

アクティブなリリースブランチとは、対応する release PR (`release` ラベル付き) が GitHub 上でまだオープンになっているブランチを指します。バックポートの自動化は実行のたびにこれらを動的に検出するため、新しいリリースが作成された場合も、古いリリースがサポート終了に達した場合も、設定を変更する必要はありません。

リリースブランチは、新しいリリースをデプロイしている期間中、**rolling-out** 状態 (release PR に `rolling-out` ラベルが付いている状態) になることがあります。ロールアウトを複雑にしないため、rolling-out 状態のブランチでは通常のバックポートは一時停止されます。バージョン固有のラベル (たとえば `v25.3-must-backport`) が付いている場合はこれが上書きされ、ロールアウト中でもバックポートが強制されます。

バージョン固有のラベルは、その PR が到達すべき *最も古い* リリースを指定します。つまり、バックポート先はそのリリース **だけでなく、それより新しいすべてのアクティブなリリースブランチ** も含まれます。たとえば、開発ブランチにマージされた PR に `v25.3-must-backport` が付いている場合、`25.3` と、それ以降のすべてのアクティブなリリース (`25.4`、`25.5`、…) にバックポートされます。複数のバージョン固有ラベルが付いている場合は、最も低いバージョンが優先されます。より新しいものはその時点ですでに対象に含まれているためです。

ラベルで指定されたリリース自体がアクティブである必要はありません。サポート終了したリリース (オープンな release PR がないもの) 向けのラベルであっても、その修正はそれ以降のすべてのアクティブなリリースに引き継がれるため、そのリリースからアップグレードした際に修正が気づかないうちに失われることはありません。たとえば、PR に `v25.12-must-backport` が付いている場合、`25.12` 自体がサポート終了に達した後でも、`26.1`、`26.2`、… へのバックポートは継続されます。

<div id="implementation">
  ## 実装
</div>

<div id="overview">
  ### 概要
</div>

バックポートの自動化は、`CherryPick` GitHub Actions ワークフロー (`.github/workflows/cherry_pick.yml`) として 1 時間ごとに実行され、`tests/ci/cherry_pick.py` に実装されています。GitHub API と、セルフホストの `style-checker-aarch64` ランナー上で実行されるローカルの git 操作によって動作します。

このプロセスは、 (original PR、リリースブランチ) の各組み合わせごとに 2 段階で進みます。

1. 実際のマージ先から競合の解消を切り離すため、**cherry-pick PR** が作成されます。競合がなければ、自動的にマージされます。
2. 実際の リリースブランチ に対して **バックポート PR** が作成され、cherry-pick された変更は 1 つの commit にまとめられます。

<div id="labels">
  ### ラベル
</div>

original PR のラベルによって、バックポートを行うかどうかと、どこに行うかが決まります。

| Label                                               | Effect                                                                                                                                                                                                                                |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pr-must-backport`                                  | すべてのアクティブな リリースブランチ にバックポートします (`rolling-out` が付いた branch はスキップされます)                                                                                                                                                            |
| `pr-must-backport-force`                            | `rolling-out` の制限を無視して、すべてのアクティブな リリースブランチ にバックポートします                                                                                                                                                                           |
| `pr-critical-bugfix`                                | `pr-must-backport` を自動的にトリガーします (`pr_labels_and_category.py` 内の `AUTO_BACKPORT` 経由)                                                                                                                                                   |
| `v{VER}-must-backport` (e.g. `v25.3-must-backport`) | その リリースブランチ **およびそれより新しいすべてのアクティブな リリースブランチ** にバックポートします。ここでのバージョンは、その PR が到達すべき*最も古い* release を示します。指定された release 自体が end-of-life であっても同様です。この種のラベルが複数ある場合は、最も低いバージョンが優先されます。これらの branch では、`rolling-out` によるスキップを上書きします |
| `pr-backports-created`                              | 必要なバックポート PR がすべて作成されると bot によって設定され、cherry-pick PR が再オープンされると解除されます                                                                                                                                                                  |
| `pr-cherrypick`                                     | bot が作成した cherry-pick PR に適用されます                                                                                                                                                                                                      |
| `pr-backport`                                       | bot が作成したバックポート PR に適用されます                                                                                                                                                                                                            |
| `do not test`                                       | CI が実行されないよう、cherry-pick PR に適用されます                                                                                                                                                                                                   |
| `rolling-out`                                       | **release PR** に設定され、その branch が現在ロールアウト中であることを示します。通常のバックポートではこの branch はスキップされます                                                                                                                                                    |

<div id="branch-and-pr-naming">
  ### ブランチ名と PR 名の命名規則
</div>

各元の PR 番号 `N` とリリースブランチ `release/X.Y` について:

* チェリーピックブランチ: `cherrypick/release/X.Y/N`
* バックポートブランチ: `backport/release/X.Y/N`
* チェリーピック PR タイトル: `Cherry pick #N to release/X.Y: <original title>`
* バックポート PR タイトル: `Backport #N to release/X.Y: <original title>`

<div id="step-by-step-process">
  ### ステップバイステップの手順
</div>

<div id="discover-active-releases">
  #### 1. アクティブなリリースを把握する
</div>

`BackportPRs.receive_release_prs` は、`release` ラベルが付いたすべてのオープンな PR を GitHub で検索します。これらの PR の head ref がリリースブランチ名 (例: `release/25.3`) に相当します。そこから、検索対象となるバージョン固有ラベルの集合を導き出します。具体的には、リポジトリ内に存在し、かつそのバージョンが最新のアクティブなリリースより新しくない、すべての `v{VER}-must-backport` ラベルです。古いラベルは、そのリリースがすでにアクティブでなくても含まれます (すべてのアクティブなリリースより新しいラベルは、どのアクティブなブランチにも展開されないためスキップされます) 。そのため、サポート終了のリリース向けにラベル付けされた PR であっても、より新しいリリースがアクティブである限り検出されます。

<div id="find-prs-to-backport">
  #### 2. バックポート対象のPRを見つける
</div>

`BackportPRs.receive_prs_for_backport` は GitHub Search API を使用して、次の条件を満たすマージ済みPRを検索します。

* 少なくとも 1 つのバックポートラベル (`pr-must-backport`、`pr-must-backport-force`、`pr-critical-bugfix`、またはバージョン固有ラベル) が付いており、
* `pr-backports-created` がまだ付与されておらず、
* いずれかのリリースブランチで見つかった最も古いコミット日時より後にマージされており、
* 過去 90 日以内に更新されている (検索クエリの効率を保つため) 。

<div id="rolling-out-branch-handling">
  #### 3. rolling-out ブランチの扱い
</div>

リリース PR に `rolling-out` ラベルが付いている場合、汎用のバックポートラベル (`pr-must-backport`、`pr-critical-bugfix`) はそのブランチをスキップします。ボットは、そのブランチ向けに以前作成された cherry-pick またはバックポート PR を、説明コメントを付けてクローズします。バージョン固有ラベル (例: `v25.3-must-backport`) は常にこの挙動より優先され、指定されたリリースと、そこから展開されるそれ以降のすべてのアクティブなリリースブランチに適用されます。`pr-must-backport-force` は、すべてのブランチで `rolling-out` チェックをバイパスします。

<div id="cherry-pick-stage">
  #### 4. Cherry-pick ステージ (`ReleaseBranch.create_cherrypick`)
</div>

cherry-pick PR がまだ存在しない各 (元の PR、リリースブランチ) の組み合わせについて:

1. リリースブランチをチェックアウトし、そこから **バックポートブランチ** (`backport/release/X.Y/N`) を作成します。
2. マージコミットの最初の親に対して `git merge -s ours` を実行し、内容変更のない合成マージベースを作成します。
3. 元の PR のマージコミットを直接指す **cherry-pick ブランチ** (`cherrypick/release/X.Y/N`) を強制的に作成します。
4. バックポートブランチに cherry-pick ブランチを `git merge --no-commit --no-ff` でマージします。
   * すでに最新状態であれば、その変更はすでにリリースブランチに取り込まれているため、完了としてマークしてスキップします。
   * それ以外の場合は (競合の有無にかかわらず) 、リセットして両方のブランチをプッシュします。
5. `cherrypick/release/X.Y/N` から `backport/release/X.Y/N` を対象とする cherry-pick PR を作成し、`pr-cherrypick` と `do not test` のラベルを付けます。
6. 必要に応じて、元の PR から `pr-bugfix` または `pr-critical-bugfix` を引き継ぎます。
7. この時点では担当者は**設定しません**。担当者を追加するのは、競合が検出された場合のみです。

<div id="auto-merge-conflict-free-cherry-pick-prs">
  #### 5. 競合のない cherry-pick PR の自動マージ
</div>

cherry-pick PR がマージ可能であれば (競合がなければ) 、ボットは GitHub API 経由で自動的にマージし、すぐにバックポート段階へ進みます。

<div id="backport-stage">
  #### 6. バックポート段階 (`ReleaseBranch.create_backport`)
</div>

cherry-pick PR がマージされたら、次の操作を行います。

1. バックポートブランチをチェックアウトし、pull します。
2. リリースブランチとバックポートブランチの merge-base を特定します。
3. merge-base に対して `git reset --soft` を実行し、cherry-pick したすべてのコミットを 1 つにまとめます。
4. バックポート PR のタイトルをメッセージにしてコミットします。
5. バックポートブランチを force-push し、実際のリリースブランチを対象とするバックポート PR を作成します。
6. PR に `pr-backport` ラベルを付けます (該当する場合は `pr-bugfix` / `pr-critical-bugfix` も付けます) 。
7. PR を、元の PR の著者、マージしたユーザー、既存の担当者 (ロボットアカウントを除く) に割り当てます。

<div id="completion">
  #### 7. 完了
</div>

特定の original PR に対応するすべてのリリースブランチへのバックポートが完了すると、ボットは元の PR に `pr-backports-created` を追加します。

<div id="pre-check">
  #### 8. 事前チェック
</div>

PR の作業を始める前に、`ReleaseBranch.pre_check` は `git merge-base --is-ancestor` を実行し、そのマージコミットがリリースブランチからまだ到達可能でないことを確認します。すでに到達可能な場合、その PR はすでにバックポート済みと見なされ、スキップされます。

<div id="stale-cherry-pick-pr-handling">
  ### Stale な cherry-pick PR の処理
</div>

`CherryPickPRs` クラスは毎時実行の開始時に実行され、次の 2 つのケースを処理します。

* **孤立した cherry-pick PR**: cherry-pick PR の リリースブランチ に対応する open な release PR が存在しなくなった場合 (つまり release が closed の場合) 、その cherry-pick PR は自動的に close されます。
* **再オープンされた cherry-pick PR**: original PR にすでに `pr-backports-created` が付いていても、それに対応する cherry-pick PR がまだ open の場合は、original PR から `pr-backports-created` label が削除され、再処理できるようになります。

手動での競合解決待ちの cherry-pick PR については、次のように処理されます。

* **3 日**間更新がない場合、bot は assignees へのメンションを含む ping コメントを投稿します。
* **7 日**間更新がない場合、bot はクローズする旨のコメントを投稿し、その PR を close します。

<div id="conflict-resolution">
  ### 競合の解決
</div>

cherry-pick で競合が発生した場合、cherry-pick PR は人手で解決できるよう、オープンのまま残されます。bot はこれを元の PR の著者、マージした人、assignees に割り当てます。競合を解決して cherry-pick PR がマージされると、bot は次の毎時実行時にバックポート PR を作成します。

バックポートを完全に破棄するには、cherry-pick PR をクローズします。bot はこれを意図的にスキップされたものとして扱います。

壊れた cherry-pick PR を最初から作り直すには:

1. cherry-pick PR から `pr-cherrypick` ラベルを削除します。
2. `cherrypick/...` ブランチを削除します。
3. 元の PR に `pr-backports-created` がある場合は削除します。

<div id="ci-for-backport-prs">
  ### バックポート PR の CI
</div>

バックポート PR はリリースブランチを対象とするため、標準のプルリクエスト用ワークフローではなく、専用の CI ワークフロー (`ci/workflows/backport_branches.py` で定義されている `BackportPR`) を使用します。このワークフローでは、CI の代表的なサブセットとして、ASan/UBSan および TSan ビルド、リリースビルド、macOS ビルド、ASan での機能テスト、TSan でのストレステスト、結合テストを実行します。また、バックポートブランチに含まれるコミット数が 1〜50 件で、変更されたファイルが少なくとも 1 つあることを検証します (`check_backport_branch.py` で強制されます) 。

<div id="authentication">
  ### 認証
</div>

このワークフローでは、Git の push 操作に SSH キー (`ROBOT_CLICKHOUSE_SSH_KEY`) を使用します。GitHub API 呼び出しの認証には `get_best_robot_token` を使用し、SSM (`/github-tokens`) に保存されたプールから、残りクォータが最も多いトークンを選択します。`ROBOT_CLICKHOUSE_COMMIT_TOKEN` は Actions ワークフローの checkout ステップで使用されるもので、API 呼び出しには使用されません。担当者を割り当てる際は、ロボットアカウント (`robot-clickhouse`、`clickhouse-gh`) は除外されます。

<div id="github-api-cache">
  ### GitHub API Cache
</div>

`GitHubCache` (`cache_utils.py` のもの) は、PyGithub の object cache を S3 に永続化し、毎時実行される処理全体での API 呼び出し回数を削減します。cache は各実行の開始時にダウンロードされ、終了時に upload されます。

<div id="error-handling">
  ### エラーハンドリング
</div>

個々のPRの処理中に発生したエラーは捕捉・記録されますが、実行が停止することはありません。すべてのPRの処理が完了した後、何らかのエラーが発生していた場合は、`BackportException` が送出されます。CIでは、これをトリガーとして `CIBuddy` 経由でチームチャットに通知されます。