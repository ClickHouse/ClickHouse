---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 50
toc_title: "\u8A2D\u5B9A\u30D5\u30A1\u30A4\u30EB"
---

# 設定ファイル {#configuration_files}

ClickHouseは複数ファイル構成管理をサポートしています。 主サーバ設定ファイルで指定することがで `/etc/clickhouse-server/config.xml`. その他のファイルは `/etc/clickhouse-server/config.d` ディレクトリ。

!!! note "メモ"
    すべての設定ファイルはxml形式である必要があります。 また、通常は同じルート要素を持つ必要があります `<yandex>`.

メイン構成ファイルで指定された一部の設定は、他の構成ファイルで上書きできます。 その `replace` または `remove` 属性は、これらの構成ファイルの要素に対して指定できます。

どちらも指定されていない場合は、重複する子の値を置き換えて、要素の内容を再帰的に結合します。

もし `replace` 指定された要素全体を指定された要素で置き換えます。

もし `remove` 指定した要素を削除します。

設定はまた定義できます “substitutions”. 要素が持っている場合 `incl` 属性は、ファイルから対応する置換は、値として使用されます。 デフォルトでは、置換を含むファイルへのパスは、 `/etc/metrika.xml`. これはで変えることができます [include\_from](server-configuration-parameters/settings.md#server_configuration_parameters-include_from) サーバー設定の要素。 置換値は、以下で指定されます `/yandex/substitution_name` このファイルの要素。 で指定された置換の場合 `incl` 存在しない、それはログに記録されます。 防ClickHouseからログイン失、置換、指定し `optional="true"` 属性(たとえば、 [マクロ](server-configuration-parameters/settings.md)).

置換はまた、飼育係から行うことができます。 これを行うには、属性を指定します `from_zk = "/path/to/node"`. 要素の値は、次の場所にあるノードの内容に置き換えられます `/path/to/node` 飼育係で また、Xmlサブツリー全体をZooKeeperノードに置くこともでき、ソース要素に完全に挿入されます。

その `config.xml` ファイルを指定することで別のconfigユーザー設定、プロファイルに割り振ります。 この設定への相対パスは `users_config` 要素。 デフォルトでは、 `users.xml`. もし `users_config` ユーザー設定、プロファイル、およびクォータは、次の場所で直接指定されます `config.xml`.

ユーザー構成は、次のような別々のファイルに分割できます `config.xml` と `config.d/`.
ディレクトリ名は、 `users_config` 設定なし `.xml` 後置は `.d`.
ディレク `users.d` デフォルトでは、 `users_config` デフォルトは `users.xml`.
たとえば、次のようにユーザーごとに別々の設定ファイルを作成できます:

``` bash
$ cat /etc/clickhouse-server/users.d/alice.xml
```

``` xml
<yandex>
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
</yandex>
```

各設定ファイルについて、サーバーも生成します `file-preprocessed.xml` 起動時のファイル。 これらのファイルには、完了した置換と上書きがすべて含まれており、情報の使用を目的としています。 ZooKeeperの置換が設定ファイルで使用されたが、ZooKeeperがサーバーの起動時に利用できない場合、サーバーは前処理されたファイルから設定をロードします。

サーバーは、設定ファイルの変更を追跡するだけでなく、置換と上書きを実行するときに使用されたファイルとzookeeperノード、およびその場でユーザーとクラスタ つまり、サーバーを再起動することなく、クラスター、ユーザー、およびその設定を変更できます。

[元の記事](https://clickhouse.tech/docs/en/operations/configuration_files/) <!--hide-->
