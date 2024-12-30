---
slug: /ja/development/adding_test_queries
sidebar_label: テストクエリの追加
sidebar_position: 63
title: ClickHouse CIにテストクエリを追加する方法
description: ClickHouseの継続的インテグレーションにテストケースを追加する手順
---

ClickHouseには何百、何千もの機能があります。すべてのコミットは、何千ものテストケースを含む複雑なテストセットによってチェックされます。

コア機能は非常によくテストされていますが、ClickHouse CIによって特異なケースやさまざまな機能の組み合わせが明らかになることがあります。

私たちが見てきたバグ/回帰の多くは、テストカバレッジが乏しい「グレーゾーン」で発生しています。

私たちは、できる限り多くのシナリオや実際に使われる機能の組み合わせをテストでカバーすることに非常に興味を持っています。

## なぜテストを追加するのか

ClickHouseのコードにテストケースを追加すべき理由/タイミング:
1) 複雑なシナリオや機能の組み合わせを使用する / あまり一般的でない特異なケースがある
2) チェンジログに通知されずにバージョン間で特定の動作が変わったことに気づく
3) ClickHouseの品質向上に貢献し、使用中の機能が将来のリリースで壊れないようにする
4) テストが追加/受入されると、その特異なケースが偶然壊れることはなくなる
5) 素晴らしいオープンソースコミュニティの一員になる
6) `system.contributors`テーブルにあなたの名前が表示される!
7) 世界を少し良くする :)

### 実施手順

#### 前提条件

Linuxマシンを使用していると仮定します（他のOSではdocker/仮想マシンを使用できます）。最新のブラウザ/インターネット接続と、基本的なLinux & SQLスキルが必要です。

高度な専門知識は必要ありません（C++やClickHouse CIの内部について知る必要はありません）。

#### 準備

1) [GitHubアカウントを作成](https://github.com/join)（まだ持っていない場合）
2) [gitをセットアップ](https://docs.github.com/en/free-pro-team@latest/github/getting-started-with-github/set-up-git)
```bash
# Ubuntuの場合
sudo apt-get update
sudo apt-get install git

git config --global user.name "John Doe" # あなたの名前を記入してください
git config --global user.email "email@example.com" # あなたのメールを記入してください

```
3) [ClickHouseプロジェクトをフォーク](https://docs.github.com/en/free-pro-team@latest/github/getting-started-with-github/fork-a-repo) - [https://github.com/ClickHouse/ClickHouse](https://github.com/ClickHouse/ClickHouse) を開き、右上のフォークボタンを押すだけです:
![フォークリポジトリ](https://github-images.s3.amazonaws.com/help/bootcamp/Bootcamp-Fork.png)

4) 自分のフォークをPCの任意のフォルダにクローン、例: `~/workspace/ClickHouse`
```
mkdir ~/workspace && cd ~/workspace
git clone https://github.com/<あなたのGitHubユーザ名>/ClickHouse
cd ClickHouse
git remote add upstream https://github.com/ClickHouse/ClickHouse
```

#### テスト用の新しいブランチ

1) 最新のClickHouseマスターから新しいブランチを作成
```
cd ~/workspace/ClickHouse
git fetch upstream
git checkout -b name_for_a_branch_with_my_test upstream/master
```

#### ClickHouseのインストールと実行

1) `clickhouse-server`をインストール（[公式ドキュメント](https://clickhouse.com/docs/ja/getting-started/install/) に従う）
2) テスト設定をインストール（Zookeeperのモック実装を使用し、いくつかの設定を調整）
```
cd ~/workspace/ClickHouse/tests/config
sudo ./install.sh
```
3) clickhouse-serverを実行
```
sudo systemctl restart clickhouse-server
```

#### テストファイルの作成

1) あなたのテストの番号を見つけます - `tests/queries/0_stateless/`内のファイルで一番大きい番号を見つけます

```sh
$ cd ~/workspace/ClickHouse
$ ls tests/queries/0_stateless/[0-9]*.reference | tail -n 1
tests/queries/0_stateless/01520_client_print_query_id.reference
```
現在のテストの最後の番号は `01520` なので、次の番号として私のテストは `01521` になります。

2) 次の番号とテストする機能の名前でSQLファイルを作成

```sh
touch tests/queries/0_stateless/01521_dummy_test.sql
```

3) 好きなエディタでSQLファイルを編集（テスト作成のヒントを参照）
```sh
vim tests/queries/0_stateless/01521_dummy_test.sql
```

4) テストを実行し、その結果をリファレンスファイルに入れます:
```
clickhouse-client -nm < tests/queries/0_stateless/01521_dummy_test.sql | tee tests/queries/0_stateless/01521_dummy_test.reference
```

5) すべてが正しいことを確認し、テスト出力が間違っている場合は（例えばバグによる）、テキストエディタを使用してリファレンスファイルを調整。

#### 良いテストを作成する方法

- テストは次の要素を持つべきです
	- **最小限** - テストする機能に関連するテーブルのみを作成し、関連しないカラムやクエリの部分を除去
	- **高速** - 数秒以内（理想的にはサブセカンド）で終了
	- **正確** - 機能が動作していない場合に失敗
    - **決定的**
	- **分離/ステートレス** - 環境に依存しない
		- タイミングに頼らない
- 特異なケース（0 / Null / 空のセット / 例外スロー）をカバーするように心がける
- クエリがエラーを返すことをテストするには、クエリの後に特別なコメントを付けることができます：`-- { serverError 60 }` または `-- { clientError 20 }`
- データベースを切り替えない（必要な場合を除く）
- 必要に応じて、同じノードに複数のテーブルレプリカを作成できます
- 必要に応じて、テストクラスター定義の一つを使用することができます（`system.clusters`を参照）
- クエリやデータの初期化には `number` / `numbers_mt` / `zeros` / `zeros_mt` などを使う
- テストの前後に作成したオブジェクトをクリーンアップ（DROP IF EXISTS） - 汚れた状態のケースを考慮
- 操作の同期モードを優先（ミューテーション、マージなど）
- `0_stateless`フォルダ内の他のSQLファイルを参考に
- テストしようとする機能/機能の組み合わせが既存のテストでカバーされていないことを確認

#### テスト命名規則

テストを適切に名前付けすることは重要です。こうすることで、clickhouse-test の呼び出し時に特定のテストサブセットをオフにできます。

| テスターフラグ | テスト名に含めるべき内容 | フラグを追加すべきとき |
|---|---|---|
| `--[no-]zookeeper`| "zookeeper" または "replica" | テストが`ReplicatedMergeTree`ファミリーのテーブルを使用する場合 |
| `--[no-]shard` | "shard" または "distributed" または "global"| テストが127.0.0.2などへの接続を使用する場合 |
| `--[no-]long` | "long" または "deadlock" または "race" | テストが60秒以上かかる場合 |

#### コミット / プッシュ / PRの作成

1) 変更をコミット & プッシュ
```sh
cd ~/workspace/ClickHouse
git add tests/queries/0_stateless/01521_dummy_test.sql
git add tests/queries/0_stateless/01521_dummy_test.reference
git commit # 可能であれば素敵なコミットメッセージを使用
git push origin HEAD
```
2) プッシュ中に表示されたリンクを使用して、メインリポジトリにPRを作成
3) PRのタイトルと内容を調整し、`Changelog category (leave one)`には `Build/Testing/Packaging Improvement` を保持し、他のフィールドを必要に応じて記入。
