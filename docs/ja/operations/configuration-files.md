---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 50
toc_title: "\u8A2D\u5B9A\u30D5\u30A1\u30A4\u30EB"
---

# 設定ファイル {#configuration_files}

ClickHouseは複数のファイル構成管理をサポートします。 主サーバ設定ファイルで指定することがで `/etc/clickhouse-server/config.xml`. その他のファイルは `/etc/clickhouse-server/config.d` ディレクトリ。

!!! note "注"
    すべての構成ファイルはXML形式である必要があります。 また、通常は同じルート要素を持つ必要があります `<clickhouse>`.

メイン構成ファイルで指定された一部の設定は、他の構成ファイルで上書きできます。 その `replace` または `remove` これらの構成ファイルの要素に属性を指定できます。

どちらも指定されていない場合は、要素の内容を再帰的に結合し、重複する子の値を置き換えます。

もし `replace` 指定された要素全体を指定された要素に置き換えます。

もし `remove` 指定されると、要素を削除します。

この設定はまた、 “substitutions”. 要素が `incl` 属性は、ファイルからの対応する置換が値として使用されます。 デフォルトでは、ファイルへのパスとの置換を行う `/etc/metrika.xml`. これはで変えることができます [include_from](server-configuration-parameters/settings.md#server_configuration_parameters-include_from) サーバー設定の要素。 置換値は、次のように指定されます `/clickhouse/substitution_name` このファイル内の要素。 で指定された置換の場合 `incl` 存在しない場合は、ログに記録されます。 ClickHouseが不足している置換をログに記録しないようにするには、 `optional="true"` 属性(たとえば、 [マクロ](server-configuration-parameters/settings.md)).

置換はZooKeeperからも実行できます。 これを行うには、属性を指定します `from_zk = "/path/to/node"`. 要素の値は、ノードの内容に置き換えられます。 `/path/to/node` 飼育係で。 また、ZooKeeperノードにXMLサブツリー全体を配置することもできます。

その `config.xml` ファイルを指定することで別のconfigユーザー設定、プロファイルに割り振ります。 この設定への相対パスは `users_config` 要素。 既定では、次のようになります `users.xml`. もし `users_config` ユーザー設定、プロファイル、およびクォータは、 `config.xml`.

ユーザー設定は、次のような別々のファイルに分割できます `config.xml` と `config.d/`.
ディレク `users_config` 設定なし `.xml` と連結された後置 `.d`.
Directory `users.d` デフォルトでは `users_config` デフォルトは `users.xml`.
たとえば、次のようにユーザーごとに別々の設定ファイルを作成できます:

``` bash
$ cat /etc/clickhouse-server/users.d/alice.xml
```

``` xml
<clickhouse>
    <users>
      <alice>
          <profile>analytics</profile>
            <networks>
                  <ip>::/0</ip>
            </networks>
          <password_sha256_hex>...</password_sha256_hex>
          <quota>analytics</quota>
      </alice>
    </users>
</clickhouse>
```

各設定ファイルでは、サーバともある `file-preprocessed.xml` 起動時のファイル。 これらのファイルには、完了したすべての置換と上書きが含まれており、情報提供を目的としています。 設定ファイルでZooKeeperの置換が使用されていても、サーバーの起動時にZooKeeperが使用できない場合、サーバーは前処理されたファイルから設定をロードします。

サーバーは、設定ファイルの変更、置換および上書きの実行時に使用されたファイルおよびZooKeeperノードを追跡し、ユーザーおよびクラスターの設定をその場で再ロード つまり、サーバーを再起動せずにクラスター、ユーザー、およびその設定を変更できます。

[元の記事](https://clickhouse.com/docs/en/operations/configuration_files/) <!--hide-->
