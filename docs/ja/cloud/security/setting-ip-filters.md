---
sidebar_label: IPフィルタの設定
slug: /ja/cloud/security/setting-ip-filters
title: IPフィルタの設定
---

## IPフィルタの設定

IPアクセスリストは、ClickHouseサービスへのトラフィックをフィルタリングし、どの送信元アドレスがClickHouseサービスに接続を許可されているかを指定します。リストは各サービスごとに設定可能です。サービスの展開時、またはその後に設定できます。プロビジョニング中にIPアクセスリストを設定しない場合や、最初のリストを変更したい場合は、サービスを選択し、**Security** タブで変更できます。

:::important
ClickHouse Cloudサービスに対するIPアクセスリストの作成をスキップした場合、サービスへのトラフィックは許可されません。
:::

## 準備

開始する前に、アクセスリストに追加すべきIPアドレスまたは範囲を収集します。リモートワーカー、オンコールの場所、VPNなどを考慮に入れてください。IPアクセスリストのユーザーインターフェースは、個々の住所とCIDR表記を受け付けます。

クラスレスドメイン間ルーティング（CIDR）表記を使用すると、従来のクラスA、B、C（8、6、または24）サブネットマスクサイズよりも小さいIPアドレス範囲を指定できます。[ARIN](https://account.arin.net/public/cidrCalculator) などの組織はCIDR電卓を提供しており、CIDR表記の詳細については、[クラスレスドメイン間ルーティング (CIDR)](https://www.rfc-editor.org/rfc/rfc4632.html) RFCをご参照ください。

## IPアクセスリストの作成または変更

ClickHouse Cloudサービスリストからサービスを選択し、**Settings** を選択します。**Security** セクションの下にIPアクセスリストがあります。テキストにあるリンクをクリックします: *このサービスに接続できる場所* **(どこからでも | 特定のx地点から)**

サイドバーが表示され、以下のオプションを設定できます:

- どこからでもサービスに対する着信トラフィックを許可
- 特定の場所からのサービスへのアクセスを許可
- サービスへのすべてのアクセスを拒否

このスクリーンショットは、「NY Office range」として説明されたIPアドレスの範囲からのトラフィックを許可するアクセスリストを示しています:

  ![既存のアクセスリスト](@site/docs/ja/cloud/security/images/ip-filtering-after-provisioning.png)

### 可能な操作

1. 追加のエントリを追加するには、**+ Add new IP** を使用

  この例では、`London server` と説明された単一のIPアドレスを追加します:

  ![アクセスリストに単一のIPを追加](@site/docs/ja/cloud/security/images/ip-filter-add-single-ip.png)

2. 既存のエントリを削除

  クロス（x）をクリックするとエントリが削除されます

3. 既存のエントリを編集

  エントリを直接変更

4. **Anywhere** からのアクセスを許可するように切り替える

  これは推奨されませんが、許可されています。ClickHouse上に構築されたアプリケーションを公開し、バックエンドのClickHouse Cloudサービスへのアクセスを制限することをお勧めします。

変更を反映させるには、**Save** をクリックする必要があります。

## 検証

フィルタを作成したら、範囲内からの接続を確認し、許可された範囲外からの接続が拒否されることを確認します。簡単な `curl` コマンドを使用して確認できます:
```bash title="許可リスト外からの接続試行が拒否される"
curl https://<HOSTNAME>.clickhouse.cloud:8443
```
```response
curl: (35) error:02FFF036:system library:func(4095):Connection reset by peer
```
または
```response
curl: (35) LibreSSL SSL_connect: SSL_ERROR_SYSCALL in connection to HOSTNAME.clickhouse.cloud:8443
```

```bash title="許可リスト内からの接続試行が許可される"
curl https://<HOSTNAME>.clickhouse.cloud:8443
```
```response
Ok.
```

## 制限事項

- 現在、IPアクセスリストはIPv4のみをサポートしています


