---
title: 通知
slug: /ja/cloud/notifications
description: あなたのClickHouse Cloudサービスの通知
keywords: [cloud, notifications]
---

ClickHouse Cloudは、あなたのサービスや組織に関連する重要なイベントについての通知を送信します。通知が送信され、設定される方法を理解するために、いくつかの概念を覚えておく必要があります：

1. **通知カテゴリ**: 請求通知やサービス関連通知などの通知のグループを指します。各カテゴリ内には、配信モードを設定できる複数の通知があります。
2. **通知の重大度**: 通知の重要度に応じて、通知の重大度は`情報`、`警告`、または`クリティカル`です。これは設定できません。
3. **通知チャンネル**: チャンネルは、UI、メール、Slackなど、通知が受信される方法を指します。これはほとんどの通知で設定可能です。

## 通知の受信

通知はさまざまなチャンネルを通じて受信できます。現在、ClickHouse CloudはメールおよびClickHouse Cloud UIを通じて通知を受信できます。左上のメニューでベルのアイコンをクリックすると現在の通知が表示されるフライアウトが開きます。フライアウトの下にある**すべて表示**ボタンをクリックすると、すべての通知の活動ログを表示するページに移動します。

<br />

<img src={require('./images/notifications-1.png').default}    
  class="image"
  alt="バックアップ設定の構成"
  style={{width: '600px'}} />

<br />

<img src={require('./images/notifications-2.png').default}    
  class="image"
  alt="バックアップ設定の構成"
  style={{width: '600px'}} />

## 通知のカスタマイズ

各通知について、通知を受信する方法をカスタマイズできます。通知フライアウトまたは通知活動ログの2番目のタブから設定画面にアクセスできます。

特定の通知の配信を設定するには、鉛筆アイコンをクリックして通知配信チャンネルを変更します。

<br />

<img src={require('./images/notifications-3.png').default}    
  class="image"
  alt="バックアップ設定の構成"
  style={{width: '600px'}} />

<br />

<img src={require('./images/notifications-4.png').default}    
  class="image"
  alt="バックアップ設定の構成"
  style={{width: '600px'}} />

<br />

:::note
**支払い失敗**などの**必須**通知は設定できません。
:::

## サポートされている通知

現在、請求関連の通知（支払い失敗、使用量が一定のしきい値を超えた場合など）やスケーリングイベントに関連する通知（スケーリング完了、スケーリングブロックなど）を送信しています。今後、バックアップ、ClickPipes、その他関連するカテゴリの通知を追加する予定です。
