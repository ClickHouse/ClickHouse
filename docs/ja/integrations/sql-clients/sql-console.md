---
sidebar_label: SQLコンソール
sidebar_position: 1
---

# SQLコンソール

SQLコンソールは、ClickHouse Cloudでデータベースを探索しクエリを実行する最速かつ最も簡単な方法です。SQLコンソールでは以下のことができます：
- ClickHouse Cloudサービスへの接続
- テーブルデータの表示、フィルタリング、並び替え
- クエリの実行と結果データの数クリックでの視覚化
- クエリをチームメンバーと共有し、より効果的にコラボレーション

## コントロールプレーンからSQLコンソールを開く

SQLコンソールは、サービスの概要画面から直接開くことができます。「接続」ボタンをクリックし、「SQLコンソールを開く」を選択してください。

  ![サービスからSQLコンソールを開く](@site/docs/ja/cloud/images/sqlconsole/open-sql-console-from-service.png)

SQLコンソールは新しいタブで開き、サービスの認証情報を入力するよう促されます：

  ![認証情報を入力](@site/docs/ja/cloud/images/sqlconsole/enter-credentials.png)

認証情報を入力した後、「接続」をクリックすると、SQLコンソールは接続と認証を試みます。成功すると、SQLコンソールのインターフェースが表示されます：

  ![認証成功](@site/docs/ja/cloud/images/sqlconsole/authentication-success.png)

### SQLコンソールを直接読み込む

SQLコンソールは、https://console.clickhouse.cloud にアクセスして直接読み込むことも可能です。ClickHouse Cloudアカウントにログインした後、サービスの一覧が表示されます。1つ選択し、サービス認証画面で認証情報を入力してください：

  ![サービスを選択](@site/docs/ja/cloud/images/sqlconsole/select-a-service.png)

:::note
組織にサービスが1つしか存在しない場合、SQLコンソールは直ちにサービス認証画面に移動します。
:::

### サービススイッチャーを使用する

SQLコンソールから直接サービスを簡単に切り替えることができます。コンソールの右上隅にあるサービススイッチャーを開き、別のサービスを選択してください：

  ![サービスを切り替える](@site/docs/ja/cloud/images/sqlconsole/switch-services.png)

## テーブルの探索

### テーブルリストとスキーマ情報の表示
ClickHouseインスタンス内のテーブルの概要は、左のサイドバーエリアに表示されます。左上のデータベースセレクターを使用して、特定のデータベース内のテーブルを表示します

  ![テーブルリストとスキーマ](@site/docs/ja/cloud/images/sqlconsole/table-list-and-schema.png)

リスト内のテーブルは展開して、カラムとタイプを確認することもできます

  ![カラムの表示](@site/docs/ja/cloud/images/sqlconsole/view-columns.png)

### テーブルデータの探索

リスト内のテーブルをクリックすると、新しいタブで開きます。テーブルビューでは、データを簡単に表示、選択、コピーできます。Microsoft ExcelやGoogle Sheetsのようなスプレッドシートアプリケーションにコピー＆ペーストする際も、構造とフォーマットは維持されます。テーブルデータのページをフリップする（30行単位でページネーションされています）際は、フッタのナビゲーションを使用してください。

  ![abc](@site/docs/ja/cloud/images/sqlconsole/abc.png)

### セルデータの検査
セルインスペクターツールを使用して、1つのセルに含まれる大量のデータを表示できます。セルを右クリックして「セルを検査」を選択することで開けます。セルインスペクタの内容は、上部右隅のコピーアイコンをクリックすることでコピーできます。

  ![セルコンテンツの検査](@site/docs/ja/cloud/images/sqlconsole/inspecting-cell-content.png)

## テーブルのフィルタリングとソート

### テーブルのソート
SQLコンソールでテーブルをソートするには、テーブルを開き、ツールバーで「ソート」ボタンを選択します。このボタンをクリックすると、ソートを構成するメニューが開きます。ソートするカラムを選択し、ソート順（昇順または降順）を構成できます。「適用」を選択するかEnterキーを押してテーブルをソートします

  ![カラムで降順にソート](@site/docs/ja/cloud/images/sqlconsole/sort-descending-on-column.png)

SQLコンソールでは、テーブルに複数のソートを追加することもできます。再度「ソート」ボタンをクリックして別のソートを追加します。注意：ソートは、ソートペインに表示される順（上から下）に適用されます。ソートを削除するには、ソートの横にある「x」ボタンをクリックします。

### テーブルのフィルタリング

SQLコンソールでテーブルをフィルタリングするには、テーブルを開き「フィルタ」ボタンを選択します。このボタンをクリックすると、フィルタを構成するメニューが開きます。フィルタリングするカラムと必要な条件を選択します。SQLコンソールはカラムに含まれるデータのタイプに応じたフィルタオプションをインテリジェントに表示します。

  ![ラジオカラムがGSMと等しいフィルタ](@site/docs/ja/cloud/images/sqlconsole/filter-on-radio-column-equal-gsm.png)

フィルタに満足したら、「適用」を選択してデータをフィルタリングします。以下のように追加のフィルタを追加することもできます。

  ![2000より大きい範囲でフィルタを追加する](@site/docs/ja/cloud/images/sqlconsole/add-more-filters.png)

ソート機能と同様に、フィルタの横にある「x」ボタンをクリックして削除します。

### 一緒にフィルタリングとソート

SQLコンソールでは、同時にテーブルのフィルタリングとソートが可能です。上記の手順を使用してすべてのフィルタとソートを追加し、「適用」ボタンをクリックしてください。

  ![一緒にフィルタリングとソート](@site/docs/ja/cloud/images/sqlconsole/filtering-and-sorting-together.png)

### フィルタとソートからクエリを作成

SQLコンソールは、一度のクリックでソートとフィルタをクエリに直接変換できます。ツールバーからお好みのソートとフィルタパラメータを使用して「クエリを作成」ボタンを選択するだけです。「クエリを作成」をクリックした後、テーブルビューに含まれるデータに対応するSQLコマンドが事前に入力された新しいクエリタブが開きます。

  ![ソートとフィルタからクエリを作成する](@site/docs/ja/cloud/images/sqlconsole/create-a-query-from-sorts-and-filters.png)

:::note
「クエリを作成」機能の使用時にフィルタとソートは必須ではありません。
:::

SQLコンソールでのクエリに関する詳細は、（リンク）クエリドキュメントをご覧ください。

## クエリの作成と実行

### クエリの作成
SQLコンソールで新しいクエリを作成する方法は2つあります。
* タブバーの「+」ボタンをクリック
* 左サイドバーのクエリリストから「新しいクエリ」ボタンを選択

  ![クエリの作成](@site/docs/ja/cloud/images/sqlconsole/creating-a-query.png)

### クエリの実行
クエリを実行するには、SQLエディタにSQLコマンドを入力し、「実行」ボタンをクリックするか、ショートカット `cmd / ctrl + enter` を使用してください。複数のコマンドを順次記述して実行するには、各コマンドの後にセミコロンを追加してください。

クエリ実行オプション
デフォルトでは、「実行」ボタンをクリックするとSQLエディタに含まれるすべてのコマンドが実行されます。SQLコンソールは他の2つのクエリ実行オプションをサポートしています：
* 選択したコマンドを実行
* カーソルのある位置のコマンドを実行

選択したコマンドを実行するには、目的のコマンドまたはシーケンスを選択し、「実行」ボタンをクリックする（または `cmd / ctrl + enter` ショートカットを使用）。選択がある場合、SQLエディタのコンテキストメニュー（エディタ内の任意の場所を右クリックして開く）から「選択したものを実行」を選択することもできます。

  ![選択したクエリを実行](@site/docs/ja/cloud/images/sqlconsole/run-selected-query.png)

カーソルの現在位置にあるコマンドを実行するには、2つの方法があります：
* 拡張実行オプションメニューから「カーソル位置で実行」を選択する（または対応する `cmd / ctrl + shift + enter` キーボードショートカットを使用）

  ![カーソル位置で実行](@site/docs/ja/cloud/images/sqlconsole/run-at-cursor-2.png)

   * SQLエディタのコンテキストメニューから「カーソル位置で実行」を選択

  ![カーソル位置で実行](@site/docs/ja/cloud/images/sqlconsole/run-at-cursor.png)

:::note
カーソル位置にあるコマンドは、実行時に黄色で点滅します。
:::

### クエリのキャンセル

クエリが実行中の場合、クエリエディタのツールバーの「実行」ボタンは「キャンセル」ボタンに置き換えられます。このボタンをクリックするか `Esc` を押してクエリをキャンセルするだけです。注意：キャンセル後も既に返されている結果は持続します。

  ![クエリをキャンセル](@site/docs/ja/cloud/images/sqlconsole/cancel-a-query.png)

### クエリの保存

クエリに名前が付けられていない場合、クエリは「無題クエリ」と呼ばれるべきです。クエリ名をクリックして変更してください。クエリの名前を変更すると、クエリが保存されます。

  ![クエリに名前を付ける](@site/docs/ja/cloud/images/sqlconsole/give-a-query-a-name.png)

また、保存ボタンまたは `cmd / ctrl + s` キーボードショートカットを使用してクエリを保存することもできます。

  ![クエリを保存する](@site/docs/ja/cloud/images/sqlconsole/save-the-query.png)

## GenAIを使用したクエリの管理

この機能を使用すると、ユーザーはクエリを自然言語の質問形式で作成し、そのコンテキストに基づいてSQLクエリを生成できます。GenAIはクエリのデバッグも支援します。

GenAIに関する詳細は、[ClickHouse CloudのGenAI対応クエリ提案の発表ブログ記事](https://clickhouse.com/blog/announcing-genai-powered-query-suggestions-clickhouse-cloud)をご覧ください。

### テーブルセットアップ

UK Price Paidの例のデータセットをインポートし、それを使用してGenAIクエリを作成しましょう。

1. ClickHouse Cloudサービスを開く。
2. *+* アイコンをクリックして新しいクエリを作成。
3. 次のコードを貼り付けて実行：

    ```sql
    CREATE TABLE uk_price_paid
    (
        price UInt32,
        date Date,
        postcode1 LowCardinality(String),
        postcode2 LowCardinality(String),
        type Enum8('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
        is_new UInt8,
        duration Enum8('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
        addr1 String,
        addr2 String,
        street LowCardinality(String),
        locality LowCardinality(String),
        town LowCardinality(String),
        district LowCardinality(String),
        county LowCardinality(String)
    )
    ENGINE = MergeTree
    ORDER BY (postcode1, postcode2, addr1, addr2);
    ```

    このクエリは約1秒で完了するはずです。完了したら、`uk_price_paid`という空のテーブルができているはずです。

4. 新しいクエリを作成して、次のクエリを貼り付けてください：

    ```sql
    INSERT INTO uk_price_paid
    WITH
       splitByChar(' ', postcode) AS p
    SELECT
        toUInt32(price_string) AS price,
        parseDateTimeBestEffortUS(time) AS date,
        p[1] AS postcode1,
        p[2] AS postcode2,
        transform(a, ['T', 'S', 'D', 'F', 'O'], ['terraced', 'semi-detached', 'detached', 'flat', 'other']) AS type,
        b = 'Y' AS is_new,
        transform(c, ['F', 'L', 'U'], ['freehold', 'leasehold', 'unknown']) AS duration,
        addr1,
        addr2,
        street,
        locality,
        town,
        district,
        county
    FROM url(
        'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv',
        'CSV',
        'uuid_string String,
        price_string String,
        time String,
        postcode String,
        a String,
        b String,
        c String,
        addr1 String,
        addr2 String,
        street String,
        locality String,
        town String,
        district String,
        county String,
        d String,
        e String'
    ) SETTINGS max_http_get_redirects=10;
    ```

このクエリは、`gov.uk` ウェブサイトからデータセットを取得します。このファイルは約4GBの大きさがあるため、クエリの完了には数分かかります。クエリが処理されると、`uk_price_paid`テーブル内にデータセット全体が含まれているはずです。

#### クエリ作成

自然言語を使用してクエリを生成しましょう。

1. **uk_price_paid** テーブルを選択し、**クエリを作成** をクリックします。
2. **SQLを生成** をクリックします。Chat-GPTにクエリを送信することに同意を求められる場合があります。この場合は **同意する** と選択してください。
3. このプロンプトを使用して自然言語のクエリを入力し、ChatGPTにSQLクエリに変換させることができます。今回の例では次のように入力します：

    > Show me the total price and total number of all uk_price_paid transactions by year.

4. コンソールは求めているクエリを生成し、新しいタブに表示します。今回の例では、GenAIが以下のクエリを作成しました：

    ```sql
    -- Show me the total price and total number of all uk_price_paid transactions by year.
    SELECT year(date), sum(price) as total_price, Count(*) as total_transactions
    FROM uk_price_paid
    GROUP BY year(date)
    ```

5. 作成されたクエリが正しいことを確認したら、**実行** をクリックして実行します。

GenAIはエクスペリメンタルな機能であることに注意してください。GenAIによって生成されたクエリを任何のデータセットで実行する際は注意してください。

### デバッグ

次に、GenAIのクエリデバッグ機能をテストしましょう。

1. *+* アイコンをクリックして新しいクエリを作成し、次のコードを貼り付けます：

    ```sql
    -- Show me the total price and total number of all uk_price_paid transactions by year.
    SELECT year(date), sum(pricee) as total_price, Count(*) as total_transactions
    FROM uk_price_paid
    GROUP BY year(date)
    ```

2. **実行** をクリックします。このクエリは`pricee`を参照しようとしているため失敗します。
3. **クエリを修正** をクリックします。
4. GenAIはクエリを修正しようとします。この場合、`pricee`を`price`に変更しました。また、このシナリオでは`toYear`がより適していると認識しました。
5. **適用** を選択して、提案された変更をクエリに反映し、**実行** をクリックします。

GenAIはエクスペリメンタルな機能であることに注意してください。GenAIによって生成されたクエリを任何のデータセットで実行する際は注意してください。

## 高度なクエリ機能

### クエリ結果の検索

クエリを実行した後、結果ペインの検索入力を使用して、返された結果セットを迅速に検索することができます。この機能は、追加の`WHERE`句の結果をプレビューしたり、特定のデータが結果セットに含まれていることを確認したりするのに便利です。検索入力に値を入力すると、結果ペインが更新され、その値と一致するエントリを含むレコードが返されます。この例では、ClickHouseリポジトリの`github_events`テーブル内の`alexey-milovidov`の全インスタンスを探してみましょう：

  ![GitHubデータを検索](@site/docs/ja/cloud/images/sqlconsole/search-github.png)

注：入力された値と一致するフィールドが返されます。たとえば、上記のスクリーンショットの3つ目のレコードは`actor_login`フィールドで`alexey-milovidov`と一致していませんが、`body`フィールドで一致しています：

  ![本文で一致](@site/docs/ja/cloud/images/sqlconsole/match-in-body.png)

### ページネーション設定の調整

デフォルトでは、クエリ結果ペインはすべての結果レコードを1つのページに表示します。より大きな結果セットでは、結果をページネーションして表示する方が見やすい場合があります。これは、結果ペインの右下隅にあるページネーションセレクターを使用して実現できます：
  ![ページネーションオプション](@site/docs/ja/cloud/images/sqlconsole/pagination.png)

ページサイズを選択すると、すぐに結果セットにページネーションが適用され、結果ペインフッタの中央にナビゲーションオプションが表示されます

  ![ページネーションナビゲーション](@site/docs/ja/cloud/images/sqlconsole/pagination-nav.png)

### クエリ結果データのエクスポート

クエリ結果セットは、SQLコンソールから直接CSV形式で簡単にエクスポートできます。右側の結果ペインツールバーの`•••`メニューを開き、「CSVとしてダウンロード」を選択してください。

  ![CSVとしてダウンロード](@site/docs/ja/cloud/images/sqlconsole/download-as-csv.png)

## クエリデータの視覚化

データの一部はチャート形式でより簡単に解釈できます。クエリ結果データからSQLコンソールで数クリックで視覚化を迅速に作成できます。例として、NYCタクシートリップの週次統計を計算するクエリを使用します：

```sql
select
   toStartOfWeek(pickup_datetime) as week,
   sum(total_amount) as fare_total,
   sum(trip_distance) as distance_total,
   count(*) as trip_total
from
   nyc_taxi
group by
   1
order by
   1 asc
```

  ![表形式のクエリ結果](@site/docs/ja/cloud/images/sqlconsole/tabular-query-results.png)

視覚化がないと、これらの結果は解釈が困難です。これをチャートに変換しましょう。

### チャートの作成

視覚化の作成を開始するには、クエリ結果ペインツールバーから「チャート」オプションを選択します。チャート構成ペインが表示されます：

  ![クエリからチャートへ切り替える](@site/docs/ja/cloud/images/sqlconsole/switch-from-query-to-chart.png)

`week`ごとの`trip_total`を追跡する単純な棒グラフを作成してみましょう。これを実現するには、`week`フィールドをx軸に、`trip_total`フィールドをy軸にドラッグします：

  ![週ごとのトリップ合計](@site/docs/ja/cloud/images/sqlconsole/trip-total-by-week.png)

ほとんどのチャートタイプは、数値軸に複数のフィールドを持つことをサポートしています。例として、`fare_total`フィールドをy軸にドラッグしてみましょう：

  ![棒グラフ](@site/docs/ja/cloud/images/sqlconsole/bar-chart.png)

### チャートのカスタマイズ

SQLコンソールは10種類のチャートタイプをサポートしており、チャート構成ペインにあるチャートタイプセレクターから選択できます。たとえば、前述のチャートタイプを棒チャートからエリアチャートに簡単に変更できます：

  ![棒グラフからエリアに変更](@site/docs/ja/cloud/images/sqlconsole/change-from-bar-to-area.png)

チャートタイトルは、データを供給しているクエリの名前に一致します。クエリ名を更新すると、チャートタイトルも更新されます：

  ![クエリ名を更新](@site/docs/ja/cloud/images/sqlconsole/update-query-name.png)

「詳細」セクションのチャート構成ペインで、より高度なチャート特性を調整することもできます。以下の設定を調整してみましょう：
- サブタイトル
- 軸タイトル
- x軸のラベルの向き

チャートはそれに応じて更新されます：

  ![サブタイトルなどを更新](@site/docs/ja/cloud/images/sqlconsole/update-subtitle-etc.png)

一部のシナリオでは、各フィールドの軸スケールを個別に調整する必要がある場合があります。これもチャート構成ペインの「詳細」セクションで、軸範囲の最小値と最大値を指定することで実現できます。上記のチャートは良好に見えますが、`trip_total`と`fare_total`フィールドの相関を示すためには、軸範囲にいくつかの調整が必要です：

  ![軸スケールの調整](@site/docs/ja/cloud/images/sqlconsole/adjust-axis-scale.png)
