---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 48
toc_title: "\u30A2\u30AF\u30BB\u30B9\u5236\u5FA1\u3068\u30A2\u30AB\u30A6\u30F3\u30C8\
  \u7BA1\u7406"
---

# アクセス制御とアカウント管理 {#access-control}

ClickHouseはに基づくアクセス制御管理を支えます [RBAC](https://en.wikipedia.org/wiki/Role-based_access_control) アプローチ

ClickHouseアクセス事業体:
- [ユーザー](#user-account-management)
- [役割](#role-management)
- [行ポリシー](#row-policy-management)
- [設定プロファイル](#settings-profiles-management)
- [クォータ](#quotas-management)

を設定することができアクセスを用いた:

-   SQL駆動型ワークフロー。

    する必要があります [有効にする](#enabling-access-control) この機能。

-   サーバ [設定ファイル](configuration-files.md) `users.xml` と `config.xml`.

SQL駆動型ワークフローの使用をお勧めします。 両方の構成方法が同時に機能するため、アカウントとアクセス権を管理するためにサーバー構成ファイルを使用する場合は、sql駆動型ワークフローに簡単

!!! note "警告"
    両方の構成方法で同じaccessエンティティを同時に管理することはできません。

## 使用法 {#access-control-usage}

既定では、ClickHouseサーバーはユーザーアカウントを提供します `default` SQL駆動型のアクセス制御とアカウント管理を使用することはできませんが、すべての権限と権限を持っています。 その `default` ユーザーアカウントは、クライアントからのログイン時や分散クエリなど、ユーザー名が定義されていない場合に使用されます。 分散クエリ処理のデフォルトのユーザーアカウントをお使いの場合、設定のサーバまたはクラスターを指定していないの [ユーザとパスワード](../engines/table-engines/special/distributed.md) プロパティ。

ClickHouseの使用を開始したばかりの場合は、次のシナリオを使用できます:

1.  [有効にする](#enabling-access-control) のためのSQL駆動型アクセス制御およびアカウント管理 `default` ユーザー。
2.  の下でログイン `default` ユーザーアカウントを作成し、必要なすべてのユーザー 管理者アカウントの作成を忘れないでください (`GRANT ALL ON *.* WITH GRANT OPTION TO admin_user_account`).
3.  [権限の制限](settings/permissions-for-queries.md#permissions_for_queries) のために `default` SQL駆動型のアクセス制御とアカウント管理をユーザーと無効にします。

### 現在のソリューションの特性 {#access-control-properties}

-   データベースとテーブルが存在しない場合でも、権限を付与できます。
-   テーブルが削除された場合、このテーブルに対応するすべての権限は取り消されません。 したがって、後で同じ名前で新しいテーブルが作成されると、すべての特権が再び実際になります。 削除されたテーブルに対応する権限を取り消すには、次のように実行する必要があります。 `REVOKE ALL PRIVILEGES ON db.table FROM ALL` クエリ。
-   特権の有効期間の設定はありません。

## ユーザー {#user-account-management}

ユーザーアカウントは、ClickHouseで誰かを承認できるアクセスエンティティです。 ユーザーアカウ:

-   識別情報。
-   [特権](../sql-reference/statements/grant.md#grant-privileges) これは、ユーザーが実行できるクエリの範囲を定義します。
-   ClickHouseサーバーへの接続が許可されているホスト。
-   付与された役割と既定の役割。
-   ユーザーのログイン時にデフォルトで適用される制約を含む設定。
-   割り当ての設定を行います。

ユーザーアカウントに対する権限は、 [GRANT](../sql-reference/statements/grant.md) クエリまたは割り当て [役割](#role-management). ユーザーから特権を取り消すために、ClickHouseは [REVOKE](../sql-reference/statements/revoke.md) クエリ。 ユーザーの権限を一覧表示するには、 - [SHOW GRANTS](../sql-reference/statements/show.md#show-grants-statement) 声明。

管理クエリ:

-   [CREATE USER](../sql-reference/statements/create.md#create-user-statement)
-   [ALTER USER](../sql-reference/statements/alter.md#alter-user-statement)
-   [DROP USER](../sql-reference/statements/misc.md#drop-user-statement)
-   [SHOW CREATE USER](../sql-reference/statements/show.md#show-create-user-statement)

### 設定の適用 {#access-control-settings-applying}

設定は、さまざまな方法で設定できます。 ユーザーログイン時に、異なるアクセスエンティティで設定が設定されている場合、この設定の値および制約は、以下の優先順位によって適用されます():

1.  ユーザーアカウント設定。
2.  ユーザーアカウントの既定のロールの設定。 一部のロールで設定が設定されている場合、設定の適用順序は未定義です。
3.  ユーザーまたはその既定のロールに割り当てられた設定プロファイルの設定。 いくつかのプロファイルで設定が設定されている場合、設定適用の順序は未定義です。
4.  すべてのサーバーにデフォルトまたは [標準プロファイル](server-configuration-parameters/settings.md#default-profile).

## 役割 {#role-management}

Roleは、ユーザーアカウントに付与できるaccessエンティティのコンテナです。

ロール:

-   [特権](../sql-reference/statements/grant.md#grant-privileges)
-   設定と制約
-   付与されたロールのリスト

管理クエリ:

-   [CREATE ROLE](../sql-reference/statements/create.md#create-role-statement)
-   [ALTER ROLE](../sql-reference/statements/alter.md#alter-role-statement)
-   [DROP ROLE](../sql-reference/statements/misc.md#drop-role-statement)
-   [SET ROLE](../sql-reference/statements/misc.md#set-role-statement)
-   [SET DEFAULT ROLE](../sql-reference/statements/misc.md#set-default-role-statement)
-   [SHOW CREATE ROLE](../sql-reference/statements/show.md#show-create-role-statement)

ロールに対する権限は、 [GRANT](../sql-reference/statements/grant.md) クエリ。 ロールClickHouseから特権を取り消すには [REVOKE](../sql-reference/statements/revoke.md) クエリ。

## 行ポリシー {#row-policy-management}

行ポリシーは、ユーザーまたはロールで使用できる行または行を定義するフィルターです。 行政政策を含むィのための特定のテーブルリストの役割および/またはユーザーはこの行政政策です。

管理クエリ:

-   [CREATE ROW POLICY](../sql-reference/statements/create.md#create-row-policy-statement)
-   [ALTER ROW POLICY](../sql-reference/statements/alter.md#alter-row-policy-statement)
-   [DROP ROW POLICY](../sql-reference/statements/misc.md#drop-row-policy-statement)
-   [SHOW CREATE ROW POLICY](../sql-reference/statements/show.md#show-create-row-policy-statement)

## 設定プロファイル {#settings-profiles-management}

設定プロファイルは [設定](settings/index.md). 設定プロファイルには、設定と制約、およびこのクォータが適用されるロールやユーザーのリストが含まれます。

管理クエリ:

-   [CREATE SETTINGS PROFILE](../sql-reference/statements/create.md#create-settings-profile-statement)
-   [ALTER SETTINGS PROFILE](../sql-reference/statements/alter.md#alter-settings-profile-statement)
-   [DROP SETTINGS PROFILE](../sql-reference/statements/misc.md#drop-settings-profile-statement)
-   [SHOW CREATE SETTINGS PROFILE](../sql-reference/statements/show.md#show-create-settings-profile-statement)

## クォータ {#quotas-management}

クォータ制限資源利用に 見る [クォータ](quotas.md).

定員の制限のために一部の時間、リストの役割および/またはユーザーはこの数量に達した場合。

管理クエリ:

-   [CREATE QUOTA](../sql-reference/statements/create.md#create-quota-statement)
-   [ALTER QUOTA](../sql-reference/statements/alter.md#alter-quota-statement)
-   [DROP QUOTA](../sql-reference/statements/misc.md#drop-quota-statement)
-   [SHOW CREATE QUOTA](../sql-reference/statements/show.md#show-create-quota-statement)

## SQL駆動型アクセス制御とアカウント管理の有効化 {#enabling-access-control}

-   設定ディレクトリ構成を保管します。

    ClickHouseは、アクセスエンティティ設定を [access\_control\_path](server-configuration-parameters/settings.md#access_control_path) サーバー構成パラメータ。

-   SQL駆動型のアクセス制御とアカウント管理を有効にします。

    デフォルトのSQL型のアクセス制御及び特別口座の口座管理オのすべてのユーザー ユーザーを設定する必要があります。 `users.xml` に1を割り当てます。 [access\_management](settings/settings-users.md#access_management-user-setting) 設定。

[元の記事](https://clickhouse.tech/docs/en/operations/access_rights/) <!--hide-->
