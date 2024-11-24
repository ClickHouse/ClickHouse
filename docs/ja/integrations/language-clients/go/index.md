---
sidebar_label: Go
sidebar_position: 1
keywords: [clickhouse, go, client, golang]
slug: /ja/integrations/go
description: ClickHouse用のGoクライアントにより、ユーザーはGo標準のdatabase/sqlインターフェースまたは最適化されたネイティブインターフェースを使用してClickHouseに接続することができます。
---

import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_native.md';

# ClickHouse Go

## 簡単な例
まずは簡単な例から始めましょう。これはClickHouseに接続し、システムデータベースからの選択を行います。始めるためには、接続情報が必要です。

### 接続情報
<ConnectionDetails />

### モジュールの初期化

```bash
mkdir clickhouse-golang-example
cd clickhouse-golang-example
go mod init clickhouse-golang-example
```

### サンプルコードをコピー

このコードを`clickhouse-golang-example`ディレクトリに`main.go`としてコピーしてください。

```go title=main.go
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func main() {
	conn, err := connect()
	if err != nil {
		panic((err))
	}

	ctx := context.Background()
	rows, err := conn.Query(ctx, "SELECT name,toString(uuid) as uuid_str FROM system.tables LIMIT 5")
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var (
			name, uuid string
		)
		if err := rows.Scan(
			&name,
			&uuid,
		); err != nil {
			log.Fatal(err)
		}
		log.Printf("name: %s, uuid: %s",
			name, uuid)
	}

}

func connect() (driver.Conn, error) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"<CLICKHOUSE_SECURE_NATIVE_HOSTNAME>:9440"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "<DEFAULT_USER_PASSWORD>",
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "an-example-go-client", Version: "0.1"},
				},
			},

			Debugf: func(format string, v ...interface{}) {
				fmt.Printf(format, v)
			},
			TLS: &tls.Config{
				InsecureSkipVerify: true,
			},
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}
```

### go mod tidyを実行

```bash
go mod tidy
```
### 接続情報を設定
以前に調べた接続情報を`main.go`の`connect()`関数で設定します:

```go
func connect() (driver.Conn, error) {
  var (
    ctx       = context.Background()
    conn, err = clickhouse.Open(&clickhouse.Options{
    #highlight-next-line
      Addr: []string{"<CLICKHOUSE_SECURE_NATIVE_HOSTNAME>:9440"},
      Auth: clickhouse.Auth{
    #highlight-start
        Database: "default",
        Username: "default",
        Password: "<DEFAULT_USER_PASSWORD>",
    #highlight-end
      },
```

### 例を実行
```bash
go run .
```
```response
2023/03/06 14:18:33 name: COLUMNS, uuid: 00000000-0000-0000-0000-000000000000
2023/03/06 14:18:33 name: SCHEMATA, uuid: 00000000-0000-0000-0000-000000000000
2023/03/06 14:18:33 name: TABLES, uuid: 00000000-0000-0000-0000-000000000000
2023/03/06 14:18:33 name: VIEWS, uuid: 00000000-0000-0000-0000-000000000000
2023/03/06 14:18:33 name: hourly_data, uuid: a4e36bd4-1e82-45b3-be77-74a0fe65c52b
```

### 詳細を学ぶ
このカテゴリーの残りのドキュメントでは、ClickHouse Goクライアントの詳細について説明します。

## ClickHouse Go クライアント

ClickHouseは2つの公式Goクライアントをサポートしています。これらのクライアントは補完的であり、意図的に異なるユースケースをサポートしています。

* [clickhouse-go](https://github.com/ClickHouse/clickhouse-go) - 高レベルの言語クライアントで、Go標準のdatabase/sqlインターフェースまたはネイティブインターフェースのいずれかをサポートしています。
* [ch-go](https://github.com/ClickHouse/ch-go) - 低レベルクライアント。ネイティブインターフェースのみ。

clickhouse-goは高レベルのインターフェースを提供し、行指向のセマンティクスを使用してデータのクエリと挿入を可能にし、データ型に対して寛容なバッチ処理を行います。ch-goは最適化された列指向のインターフェースを提供し、タイプの厳密さと複雑な使用を犠牲にして低CPUとメモリオーバーヘッドで高速なデータブロックのストリーミングを行います。

version 2.3から、clickhouse-goは低レベル関数（エンコーディング、デコーディング、圧縮など）にch-goを利用しています。clickhouse-goはまた、Goの`database/sql`インターフェース規格をサポートしています。どちらのクライアントもエンコードのためにネイティブフォーマットを使用して、最適なパフォーマンスを提供し、ネイティブClickHouseプロトコルを介して通信できます。clickhouse-goは、ユーザーがプロキシまたは負荷分散の要件を持っている場合にはHTTPを輸送機構としてもサポートしています。

クライアントライブラリを選択する際には、各クライアントの利点と欠点を認識しておく必要があります。詳細はChoosing a Client Libraryを参照してください。

|               | ネイティブフォーマット | ネイティブプロトコル | HTTPプロトコル | 行指向API | 列指向API | タイプの柔軟性 | 圧縮 | クエリプレースホルダ |
|:-------------:|:------------------:|:------------------:|:--------------:|:--------------:|:-------------:|:------------------:|:-------------------:|:------------------:|
| clickhouse-go |       ✅            |        ✅           |       ✅         |          ✅         |         ✅          |         ✅        |      ✅          |          ✅       |
|     ch-go     |       ✅            |        ✅           |               |                    |         ✅          |                  |      ✅          |                    |

## クライアントの選択

クライアントライブラリの選択は、使用パターンと最適なパフォーマンスの要求に依存します。毎秒数百万件の挿入が必要な挿入中心のユースケースでは、低レベルクライアント[ch-go](https://github.com/ClickHouse/ch-go)の使用をお勧めします。このクライアントは、ClickHouseネイティブフォーマットが要求する行指向フォーマットから列へのデータ変換の関連オーバーヘッドを回避し、簡易化のために`interface{}`（`any`）タイプのリフレクションと使用を回避します。

集計を中心としたクエリワークロードまたはスループットが低い挿入ワークロードに関しては、[clickhouse-go](https://github.com/ClickHouse/clickhouse-go)が使い慣れた`database/sql`インターフェースとよりシンプルな行セマンティクスを提供します。ユーザーはオプションでHTTPを輸送プロトコルとして使用し、構造体との間で行をマーシャリングするためのヘルパー関数を利用することができます。

## clickhouse-go クライアント

clickhouse-goクライアントは、ClickHouseと通信するための2つのAPIインターフェースを提供します：

* ClickHouseクライアント専用API
* `database/sql`標準 - Golangが提供するSQLデータベース周りのジェネリックインターフェース。

`database/sql`は、データベースに依存しないインターフェースを提供し、開発者がデータストアを抽象化することを可能にしますが、パフォーマンスに影響を与えるいくつかのタイプとクエリセマンティクスを強制します。このため、パフォーマンスが重要な場合(https://github.com/clickHouse/clickHouse-go#benchmark)、クライアント専用APIの使用が推奨されます。ただし、複数データベースをサポートするツールにClickHouseを統合したいユーザーは標準インターフェースの使用を好むかもしれません。

いずれのインターフェースもデータをエンコードする際に[native format](/docs/ja/native-protocol/basics.md)とネイティブプロトコルを使用します。加えて、標準インターフェースはHTTPを介した通信もサポートしています。

|                    | ネイティブフォーマット | ネイティブプロトコル | HTTPプロトコル | バルク書き込みサポート | 構造体マーシャリング | 圧縮 | クエリプレースホルダ |
|:------------------:|:------------------:|:------------------:|:--------------:|:------------------:|:-----------------:|:-----------:|:------------------:|
|   ClickHouse API   |       ✅            |        ✅           |               |          ✅         |         ✅         |      ✅      |          ✅         |
| `database/sql` API |       ✅            |        ✅           |       ✅       |          ✅         |                   |      ✅      |          ✅         |

## インストール

ドライバのv1は非推奨となっており、機能更新や新しいClickHouseタイプのサポートは行われません。ユーザーは、パフォーマンスが優れているv2への移行を推奨します。

2.xバージョンのクライアントをインストールするには、パッケージをgo.modファイルに追加してください：

`require github.com/ClickHouse/clickhouse-go/v2 main`

または、リポジトリをクローンしてください：

```bash
git clone --branch v2 https://github.com/clickhouse/clickhouse-go.git $GOPATH/src/github
```

他のバージョンをインストールするには、パスまたはブランチ名を必要に応じて修正してください。

```bash
mkdir my-clickhouse-app && cd my-clickhouse-app

cat > go.mod <<-END
  module my-clickhouse-app

  go 1.18

  require github.com/ClickHouse/clickhouse-go/v2 main
END

cat > main.go <<-END
  package main

  import (
    "fmt"
    "github.com/ClickHouse/clickhouse-go/v2"
  )

  func main() {
   conn, _ := clickhouse.Open(&clickhouse.Options{Addr: []string{"127.0.0.1:9000"}})
    v, _ := conn.ServerVersion()
    fmt.Println(v.String())
  }
END

go mod tidy
go run main.go

```

### バージョニングと互換性

クライアントはClickHouseとは独立してリリースされます。2.xは現在の主な開発対象です。2.xの全バージョンは互いに互換性があるべきです。

#### ClickHouse互換性

クライアントは以下をサポートしています：

- ここに記録されているClickHouseの全てのサポートされるバージョン。[ここ](https://github.com/ClickHouse/ClickHouse/blob/master/SECURITY.md)に記載されている通りです。ClickHouseのバージョンがもはやサポートされていない場合、それに対してクライアントリリースもアクティブにはテストされなくなります。
- クライアントのリリース日から2年以内の全てのClickHouseバージョン。注意：LTSバージョンのみがアクティブにテストされます。

#### Golang互換性

| クライアントバージョン | Golangバージョン |
|:--------------:|:---------------:|
|  => 2.0 <= 2.2 |    1.17, 1.18   |
|     >= 2.3     |       1.18      |


## ClickHouse クライアント API

ClickHouseクライアントAPI用の全てのコード例は[こちら](https://github.com/ClickHouse/clickhouse-go/tree/main/examples)にあります。

### 接続

以下の例はClickHouseに接続し、サーバーバージョンを返す例を示しています。ClickHouseがセキュリティ保護されておらず、デフォルトのユーザーでアクセス可能な場合を想定しています。

注意：デフォルトのネイティブポートを使用して接続しています。

```go
conn, err := clickhouse.Open(&clickhouse.Options{
    Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
    Auth: clickhouse.Auth{
        Database: env.Database,
        Username: env.Username,
        Password: env.Password,
    },
})
if err != nil {
    return err
}
v, err := conn.ServerVersion()
fmt.Println(v)
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/connect.go)

**後続の例では、明示的に示されていない限り、ClickHouseの`conn`変数が作成され、利用可能であると仮定します。**

#### 接続設定

接続を開く際に、Options構造体を使用してクライアントの動作を制御できます。利用可能な設定は以下の通りです：

* `Protocol` - ネイティブまたはHTTP。現在、HTTPは[database/sql API](#databasesql-api)のみサポートされています。
* `TLS` - TLSオプション。非nilの値がTLSを有効にします。[Using TLS](#using-tls)を参照。
* `Addr` - アドレス（ポート込み）のスライス。
* `Auth` - 認証詳細。[Authentication](#authentication)を参照。
* `DialContext` - 接続の確立方法を決定するカスタムダイヤル関数。
* `Debug` - デバッグを有効にするためのtrue/false。
* `Debugf` - デバッグ出力を消費する関数を提供します。`debug`がtrueに設定されている必要があります。
* `Settings` - ClickHouseの設定のマップ。これらは全てのClickHouseクエリに適用されます。[Using Context](#using-context)を使用して、クエリごとに設定を設定できます。
* `Compression` - ブロックの圧縮を有効にします。[Compression](#compression)を参照。
* `DialTimeout` - 接続の確立にかかる最大時間。デフォルトは`1秒`。
* `MaxOpenConns` - 任意の時点で使用できる最大接続数です。アイドルプールにある接続は増減する可能性がありますが、任意の時点で使用できる接続はこの数に限定されます。デフォルトはMaxIdleConns+5です。
* `MaxIdleConns` - プールに保持する接続数。可能な場合、接続は再利用されます。デフォルトは`5`。
* `ConnMaxLifetime` - 接続を利用可能な状態で保持する最長寿命。デフォルトは1時間です。この時間の後、接続は破棄され、必要に応じて新しい接続がプールに追加されます。
* `ConnOpenStrategy` - ノードアドレスリストをどのように消費し、接続を確立するかを決定します。[Connecting to Multiple Nodes](#connecting-to-multiple-nodes)を参照。
* `BlockBufferSize` - 一度にバッファにデコードするブロックの最大数。大きな値は並列化を増やしますが、メモリを犠牲にする可能性があります。ブロックサイズはクエリに依存するため、接続時にこれを設定できますが、返すデータに基づいてクエリごとにオーバーライドすることをお勧めします。デフォルトは`2`です。

```go
conn, err := clickhouse.Open(&clickhouse.Options{
    Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
    Auth: clickhouse.Auth{
        Database: env.Database,
        Username: env.Username,
        Password: env.Password,
    },
    DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
        dialCount++
        var d net.Dialer
        return d.DialContext(ctx, "tcp", addr)
    },
    Debug: true,
    Debugf: func(format string, v ...interface{}) {
        fmt.Printf(format, v)
    },
    Settings: clickhouse.Settings{
        "max_execution_time": 60,
    },
    Compression: &clickhouse.Compression{
        Method: clickhouse.CompressionLZ4,
    },
    DialTimeout:      time.Duration(10) * time.Second,
    MaxOpenConns:     5,
    MaxIdleConns:     5,
    ConnMaxLifetime:  time.Duration(10) * time.Minute,
    ConnOpenStrategy: clickhouse.ConnOpenInOrder,
    BlockBufferSize: 10,
})
if err != nil {
    return err
}
```
[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/connect_settings.go)

#### 接続プール

クライアントは接続プールを維持し、必要に応じてクエリ間でこれらを再利用します。任意の時点で使用される接続は`MaxOpenConns`で制御され、プールの最大サイズは`MaxIdleConns`で制御されます。クライアントはクエリの実行ごとにプールから接続を取得し、それを再利用のためにプールに返却します。接続はバッチの期間中使用され、`Send()`で解放されます。

プール内の同じ接続が後続のクエリに使用される保証はありませんが、ユーザーが`MaxOpenConns=1`を設定した場合は例外です。これはめったに必要ありませんが、一時テーブルを使用している場合には必要になる場合があります。

また`ConnMaxLifetime`はデフォルトで1時間であることに注意してください。このことが原因で、ノードがクラスターを離れる際にClickHouseへの負荷が不均衡になることがあります。これは、ノードが利用不能になると、他のノードに接続がバランスされるためです。これらの接続は持続し、問題のあるノードがクラスターに戻ってもデフォルトで1時間更新されません。負荷の高いワークロードの場合にはこの値を下げることを検討してください。

### TLSを使用する

低レベルでは、全てのクライアント接続メソッド（DSN/OpenDB/Open）は安全な接続を確立するために[Go tls パッケージ](https://pkg.go.dev/crypto/tls)を使用します。Options構造体に非nilの`tls.Config`ポインターが含まれている場合、クライアントはTLSを使用します。

```go
env, err := GetNativeTestEnvironment()
if err != nil {
    return err
}
cwd, err := os.Getwd()
if err != nil {
    return err
}
t := &tls.Config{}
caCert, err := ioutil.ReadFile(path.Join(cwd, "../../tests/resources/CAroot.crt"))
if err != nil {
    return err
}
caCertPool := x509.NewCertPool()
successful := caCertPool.AppendCertsFromPEM(caCert)
if !successful {
    return err
}
t.RootCAs = caCertPool
conn, err := clickhouse.Open(&clickhouse.Options{
    Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.SslPort)},
    Auth: clickhouse.Auth{
        Database: env.Database,
        Username: env.Username,
        Password: env.Password,
    },
    TLS: t,
})
if err != nil {
    return err
}
v, err := conn.ServerVersion()
if err != nil {
    return err
}
fmt.Println(v.String())
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/ssl.go)

この最小限の`TLS.Config`は通常、ClickHouseサーバーの安全なネイティブポート（通常は9440）に接続するのに十分です。ClickHouseサーバーに有効な証明書がない（期限切れ、ホスト名が間違っている、公開的に認識されるルート認証局によって署名されていない）場合は、InsecureSkipVerifyをtrueにすることもできますが、これは強く推奨されません。

```go
conn, err := clickhouse.Open(&clickhouse.Options{
    Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.SslPort)},
    Auth: clickhouse.Auth{
        Database: env.Database,
        Username: env.Username,
        Password: env.Password,
    },
    TLS: &tls.Config{
        InsecureSkipVerify: true,
    },
})
if err != nil {
    return err
}
v, err := conn.ServerVersion()
```
[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/ssl_no_verify.go)

追加のTLSパラメータが必要な場合は、アプリケーションコードで`tls.Config`構造体の必要なフィールドを設定する必要があります。これには、特定の暗号スイートの指定、特定のTLSバージョン（たとえば1.2または1.3）の強制、内部CA証明書チェーンの追加、ClickHouseサーバーによって要求された場合のクライアント証明書（および秘密鍵）の追加、そしてより専門的なセキュリティ設定の他のオプションが含まれます。

### 認証

接続情報でAuth構造体を指定して、ユーザー名とパスワードを指定します。

```go
conn, err := clickhouse.Open(&clickhouse.Options{
    Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
    Auth: clickhouse.Auth{
        Database: env.Database,
        Username: env.Username,
        Password: env.Password,
    },
})
if err != nil {
    return err
}
if err != nil {
    return err
}
v, err := conn.ServerVersion()
```
[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/auth.go)

### 複数のノードへの接続

`Addr`構造体を介して複数のアドレスを指定できます。

```go
conn, err := clickhouse.Open(&clickhouse.Options{
    Addr: []string{"127.0.0.1:9001", "127.0.0.1:9002", fmt.Sprintf("%s:%d", env.Host, env.Port)},
    Auth: clickhouse.Auth{
        Database: env.Database,
        Username: env.Username,
        Password: env.Password,
    },
})
if err != nil {
    return err
}
v, err := conn.ServerVersion()
if err != nil {
    return err
}
fmt.Println(v.String())
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/1c0d81d0b1388dbb9e09209e535667df212f4ae4/examples/clickhouse_api/multi_host.go#L26-L45)

2つの接続戦略が利用可能です：

* `ConnOpenInOrder`（デフォルト） - アドレスは順番に消費されます。後のアドレスは、リスト内のより早いアドレスを使用して接続できない場合にのみ利用されます。これは事実上のフェイルオーバー戦略です。
* `ConnOpenRoundRobin` - ラウンドロビン戦略を使用してアドレス間の負荷をバランスします。

これはオプション`ConnOpenStrategy`を介して制御できます。

```go
conn, err := clickhouse.Open(&clickhouse.Options{
    Addr:             []string{"127.0.0.1:9001", "127.0.0.1:9002", fmt.Sprintf("%s:%d", env.Host, env.Port)},
    ConnOpenStrategy: clickhouse.ConnOpenRoundRobin,
    Auth: clickhouse.Auth{
        Database: env.Database,
        Username: env.Username,
        Password: env.Password,
    },
})
if err != nil {
    return err
}
v, err := conn.ServerVersion()
if err != nil {
    return err
}
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/1c0d81d0b1388dbb9e09209e535667df212f4ae4/examples/clickhouse_api/multi_host.go#L50-L67)

### 実行

任意のステートメントを`Exec`メソッドを介して実行できます。これはDDLや単純なステートメントに便利です。大きな挿入やクエリの繰り返しには使用しないでください。

```go
conn.Exec(context.Background(), `DROP TABLE IF EXISTS example`)
err = conn.Exec(context.Background(), `
    CREATE TABLE IF NOT EXISTS example (
        Col1 UInt8,
        Col2 String
    ) engine=Memory
`)
if err != nil {
    return err
}
conn.Exec(context.Background(), "INSERT INTO example VALUES (1, 'test-1')")
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/exec.go)

クエリに特定の設定を渡すためにContextをクエリに渡す機能に注意してください。詳細は[Using Context](#using-context)でご覧ください。

### バッチ挿入

大量の行を挿入するために、クライアントはバッチセマンティクスを提供します。これには、バッチの準備と、それへの行の追加が必要です。送信は最終的に`Send()`メソッドを介して行われます。バッチはSendが実行されるまでメモリ内に保持されます。

```go
conn, err := GetNativeConnection(nil, nil, nil)
if err != nil {
    return err
}
ctx := context.Background()
defer func() {
    conn.Exec(ctx, "DROP TABLE example")
}()
conn.Exec(context.Background(), "DROP TABLE IF EXISTS example")
err = conn.Exec(ctx, `
    CREATE TABLE IF NOT EXISTS example (
            Col1 UInt8
        , Col2 String
        , Col3 FixedString(3)
        , Col4 UUID
        , Col5 Map(String, UInt8)
        , Col6 Array(String)
        , Col7 Tuple(String, UInt8, Array(Map(String, String)))
        , Col8 DateTime
    ) Engine = Memory
`)
if err != nil {
    return err
}


batch, err := conn.PrepareBatch(ctx, "INSERT INTO example")
if err != nil {
    return err
}
for i := 0; i < 1000; i++ {
    err := batch.Append(
        uint8(42),
        "ClickHouse",
        "Inc",
        uuid.New(),
        map[string]uint8{"key": 1},             // Map(String, UInt8)
        []string{"Q", "W", "E", "R", "T", "Y"}, // Array(String)
        []interface{}{ // Tuple(String, UInt8, Array(Map(String, String)))
            "String Value", uint8(5), []map[string]string{
                {"key": "value"},
                {"key": "value"},
                {"key": "value"},
            },
        },
        time.Now(),
    )
    if err != nil {
        return err
    }
}
return batch.Send()
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/batch.go)

ClickHouseの推奨事項は[こちら](https://clickhouse.com/docs/ja/about-us/performance/#performance-when-inserting-data)を参照してください。バッチはgoルーチン間で共有しないでください - ルーチンごとに別々のバッチを構築してください。

上記の例から、行を追加する際には変数タイプがカラムタイプと一致する必要があることに注意してください。マッピングは通常明らかですが、このインターフェースは柔軟であるように努めており、精度の損失がない限り型が変換されます。たとえば、次の例では、datetime64に文字列を挿入することを示しています。

```go
batch, err := conn.PrepareBatch(ctx, "INSERT INTO example")
if err != nil {
    return err
}
for i := 0; i < 1000; i++ {
    err := batch.Append(
        "2006-01-02 15:04:05.999",
    )
    if err != nil {
        return err
    }
}
return batch.Send()
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/type_convert.go)

各カラムタイプに対する対応するgoタイプの全体的な要約については、 [Type Conversions](#type-conversions)をご参照ください。

特定の変換のサポートが必要な場合は、問題を報告していただければと思います。

### クエリの実行

ユーザーは`QueryRow`メソッドを使用して単一の行をクエリするか、`Query`を介して結果セットを繰り返し処理するカーソルを取得することができます。前者はデータをシリアライズするための宛先を受け取るのに対し、後者は各行に対して`Scan`を呼び出す必要があります。

```go
row := conn.QueryRow(context.Background(), "SELECT * FROM example")
var (
    col1             uint8
    col2, col3, col4 string
    col5             map[string]uint8
    col6             []string
    col7             []interface{}
    col8             time.Time
)
if err := row.Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8); err != nil {
    return err
}
fmt.Printf("row: col1=%d, col2=%s, col3=%s, col4=%s, col5=%v, col6=%v, col7=%v, col8=%v\n", col1, col2, col3, col4, col5, col6, col7, col8)
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/query_row.go)

```go
rows, err := conn.Query(ctx, "SELECT Col1, Col2, Col3 FROM example WHERE Col1 >= 2")
if err != nil {
    return err
}
for rows.Next() {
    var (
        col1 uint8
        col2 string
        col3 time.Time
    )
    if err := rows.Scan(&col1, &col2, &col3); err != nil {
        return err
    }
    fmt.Printf("row: col1=%d, col2=%s, col3=%s\n", col1, col2, col3)
}
rows.Close()
return rows.Err()
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/query_rows.go)

いずれの場合も、対応するカラム値をシリアライズするためにポインタを変数に渡す必要があることに注意してください。これらは`SELECT`ステートメントで指定された順序で渡す必要があります - デフォルトでは、上記のように`SELECT *`がある場合、カラムの宣言順序が使用されます。

挿入と同様に、`Scan`メソッドはターゲット変数が適切なタイプである必要があります。これも柔軟であるよう努めていますが、UUIDカラムを文字列変数に読み込むことなど、型が可能な限り変換されるようサポートしています。さまざまなカラムタイプに対するgolangタイプの一覧については、[Type Conversions](#type-conversions)をご参照ください。

最後に、`Query`と`QueryRow`メソッドにContextを渡す能力に注意してください。これは、クエリレベルの設定に使用できます。詳細は[Using Context](#using-context)で参照してください。

### 非同期挿入

非同期挿入はAsyncメソッドを介してサポートされています。これは、クライアントがデータを受信した後に応答するか、サーバーの挿入が完了するまで待機するかを指定することができます。これにより[wait_for_async_insert](https://clickhouse.com/docs/ja/operations/settings/settings/#wait-for-async-insert)を制御します。

```go
conn, err := GetNativeConnection(nil, nil, nil)
if err != nil {
    return err
}
ctx := context.Background()
if err := clickhouse_tests.CheckMinServerServerVersion(conn, 21, 12, 0); err != nil {
    return nil
}
defer func() {
    conn.Exec(ctx, "DROP TABLE example")
}()
conn.Exec(ctx, `DROP TABLE IF EXISTS example`)
const ddl = `
    CREATE TABLE example (
            Col1 UInt64
        , Col2 String
        , Col3 Array(UInt8)
        , Col4 DateTime
    ) ENGINE = Memory
`
if err := conn.Exec(ctx, ddl); err != nil {
    return err
}
for i := 0; i < 100; i++ {
    if err := conn.AsyncInsert(ctx, fmt.Sprintf(`INSERT INTO example VALUES (
        %d, '%s', [1, 2, 3, 4, 5, 6, 7, 8, 9], now()
    )`, i, "Golang SQL database driver"), false); err != nil {
        return err
    }
}
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/async.go)

### 列指向の挿入

カラム形式で挿入することができます。データが既にこの構造に整理されている場合において、パフォーマンスの改善が予想されます。

```go
batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO example")
if err != nil {
    return err
}
var (
    col1 []uint64
    col2 []string
    col3 [][]uint8
    col4 []time.Time
)
for i := 0; i < 1_000; i++ {
    col1 = append(col1, uint64(i))
    col2 = append(col2, "Golang SQL database driver")
    col3 = append(col3, []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9})
    col4 = append(col4, time.Now())
}
if err := batch.Column(0).Append(col1); err != nil {
    return err
}
if err := batch.Column(1).Append(col2); err != nil {
    return err
}
if err := batch.Column(2).Append(col3); err != nil {
    return err
}
if err := batch.Column(3).Append(col4); err != nil {
    return err
}
return batch.Send()
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/columnar_insert.go)

### 構造体の使用

ユーザーにとって、Golangの構造体はClickHouseのデータの行を論理的に表現するものです。ネイティブインターフェースは、これを支援するための便利な関数を提供しています。

#### シリアライズ付き選択

Selectメソッドを使用すると、応答行を単一の呼び出しで構造体のスライスにマーシャルできます。

```go
var result []struct {
    Col1           uint8
    Col2           string
    ColumnWithName time.Time `ch:"Col3"`
}

if err = conn.Select(ctx, &result, "SELECT Col1, Col2, Col3 FROM example"); err != nil {
    return err
}

for _, v := range result {
    fmt.Printf("row: col1=%d, col2=%s, col3=%s\n", v.Col1, v.Col2, v.ColumnWithName)
}
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/select_struct.go)

#### Scan 構造体

ScanStructは、クエリから単一の行を構造体にマーシャルすることを可能にします。

```go
var result struct {
    Col1  int64
    Count uint64 `ch:"count"`
}
if err := conn.QueryRow(context.Background(), "SELECT Col1, COUNT() AS count FROM example WHERE Col1 = 5 GROUP BY Col1").ScanStruct(&result); err != nil {
    return err
}
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/scan_struct.go)

#### 構造体の追加

AppendStructは[batch](#batch-insert)に構造体を追加して解釈できます。これは、構造体のカラムが名前と型の両方でテーブルと一致する必要があります。全てのカラムが同等の構造体フィールドを持つ必要がありますが、一部の構造体フィールドは同等のカラム表現を持っていない場合もあります。これらは単に無視されます。

```go
batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO example")
if err != nil {
    return err
}
for i := 0; i < 1_000; i++ {
    err := batch.AppendStruct(&row{
        Col1:       uint64(i),
        Col2:       "Golang SQL database driver",
        Col3:       []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9},
        Col4:       time.Now(),
        ColIgnored: "this will be ignored",
    })
    if err != nil {
        return err
    }
}
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/append_struct.go)

### タイプ変換

クライアントは挿入とレスポンスのマーシャリングに関して可能な限り柔軟に対応することを目指しています。多くの場合、ClickHouseのカラムタイプに相当するGolangタイプが存在します。例：[UInt64](https://clickhouse.com/docs/ja/sql-reference/data-types/int-uint/) to [uint64](https://pkg.go.dev/builtin#uint64)。これらの論理的なマッピングは常にサポートされるべきです。ユーザーは、変数をカラムに挿入するか、受信データをまず変換することによって、その変換を行える場合において変数タイプを利用することを希望するかもしれません。この透明な変換は、精度の損失が許されない場合にのみ実行されます。例えば、uint32はUInt64カラムから受信データを得るために使用することはできません。逆に、指定フォーマット要件を満たせば、datetime64フィールドに文字列を挿入することもできます。

現在サポートされているプリミティブタイプの変換については[こちら](https://github.com/ClickHouse/clickhouse-go/blob/main/TYPES.md)を参照してください。

この作業は進行中であり、挿入タイミング（`Append`/`AppendRow`）と読み取り時（`Scan`でご利用いただけます）に分けられます。特定の変換のサポートが必要な場合は、問題を報告してください。

### 複雑なタイプ

#### 日付/日時タイプ

ClickHouse goクライアントは`Date`、`Date32`、`DateTime`、および`DateTime64`の日付/日時タイプをサポートしています。日付は`2006-01-02`の形式の文字列か、ネイティブGoの`time.Time{}`または`sql.NullTime`を使って挿入できます。日時も同様ですが、`2006-01-02 15:04:05`のフォーマットの文字列としてする必要があります。timezoneオフセットがある場合は、`2006-01-02 15:04:05 +08:00`のようにしてください。`time.Time{}`と`sql.NullTime`は読み取り時にもサポートされており、`sql.Scanner`インターフェースの任意の実装もサポートしています。

Timezone情報の扱いは、ClickHouseのタイプと値が挿入/読み取りされているかに依存します：

* **DateTime/DateTime64**
    * **挿入**時には、値はUNIXタイムスタンプ形式でClickHouseに送信されます。タイムゾーンが提供されない場合、クライアントのローカルタイムゾーンが仮定されます。`time.Time{}`や`sql.NullTime`は、対応する論理に変換されます。
    * **選択**時には、カラムのtimezoneが設定されている場合、そのtimezoneが`time.Time`値を返す際に使われます。されていない場合は、サーバーのtimezoneが使われます。
* **Date/Date32**
    * **挿入**時には、日付はUNIXタイムスタンプへの変換時にタイムゾーンを考慮されます、すなわち、タイムゾーンでオフセットされた上で日付として保存されます。日付はClickHouseにロケールがありません。これは、文字列内に指定されていない場合、ローカルタイムゾーンが使われます。
    * **選択**時には、日時が`time.Time{}`または`sql.NullTime{}`インスタンスに読み込まれるときにtimezone情報がありません。

#### 配列

配列はスライスとして挿入されるべきです。要素の型ルールは[プリミティブタイプ](#type-conversions)に対するものと一致し、可能な場合は要素が変換されます。

Scan時には、スライスへのポインタを指定する必要があります。

```go
batch, err := conn.PrepareBatch(ctx, "INSERT INTO example")
if err != nil {
    return err
}
var i int64
for i = 0; i < 10; i++ {
    err := batch.Append(
        []string{strconv.Itoa(int(i)), strconv.Itoa(int(i + 1)), strconv.Itoa(int(i + 2)), strconv.Itoa(int(i + 3))},
        [][]int64{{i, i + 1}, {i + 2, i + 3}, {i + 4, i + 5}},
    )
    if err != nil {
        return err
    }
}
if err := batch.Send(); err != nil {
    return err
}
var (
    col1 []string
    col2 [][]int64
)
rows, err := conn.Query(ctx, "SELECT * FROM example")
if err != nil {
    return err
}
for rows.Next() {
    if err := rows.Scan(&col1, &col2); err != nil {
        return err
    }
    fmt.Printf("row: col1=%v, col2=%v\n", col1, col2)
}
rows.Close()
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/array.go)

#### マップ

マップは、前述の[タイプ変換](#type-conversions)で定義された型ルールに準拠したGolangマップとして挿入されるべきです。

```go
batch, err := conn.PrepareBatch(ctx, "INSERT INTO example")
if err != nil {
    return err
}
var i int64
for i = 0; i < 10; i++ {
    err := batch.Append(
        map[string]uint64{strconv.Itoa(int(i)): uint64(i)},
        map[string][]string{strconv.Itoa(int(i)): {strconv.Itoa(int(i)), strconv.Itoa(int(i + 1)), strconv.Itoa(int(i + 2)), strconv.Itoa(int(i + 3))}},
        map[string]map[string]uint64{strconv.Itoa(int(i)): {strconv.Itoa(int(i)): uint64(i)}},
    )
    if err != nil {
        return err
    }
}
if err := batch.Send(); err != nil {
    return err
}
var (
    col1 map[string]uint64
    col2 map[string][]string
    col3 map[string]map[string]uint64
)
rows, err := conn.Query(ctx, "SELECT * FROM example")
if err != nil {
    return err
}
for rows.Next() {
    if err := rows.Scan(&col1, &col2, &col3); err != nil {
        return err
    }
    fmt.Printf("row: col1=%v, col2=%v, col3=%v\n", col1, col2, col3)
}
rows.Close()
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/map.go)

#### タプル

タプルは任意の長さのカラムのグループを表します。カラムは明示的に名前を付けるか、型だけを指定することができます。例：

```sql
// 非名
Col1 Tuple(String, Int64)

// 名
Col2 Tuple(name String, id Int64, age uint8)
```

これらのアプローチの中で、名のタプルはより柔軟性があります。非名のタプルはスライスを使用して挿入および読み取りする必要がありますが、名のタプルはマップにも互換性があります。

```go
if err = conn.Exec(ctx, `
    CREATE TABLE example (
            Col1 Tuple(name String, age UInt8),
            Col2 Tuple(String, UInt8),
            Col3 Tuple(name String, id String)
        )
        Engine Memory
    `); err != nil {
    return err
}

defer func() {
    conn.Exec(ctx, "DROP TABLE example")
}()
batch, err := conn.PrepareBatch(ctx, "INSERT INTO example")
if err != nil {
    return err
}
// 名と非名のどちらもスライスで追加できる
if err = batch.Append([]interface{}{"Clicky McClickHouse", uint8(42)}, []interface{}{"Clicky McClickHouse Snr", uint8(78)}, []string{"Dale", "521211"}); err != nil {
    return err
}
if err = batch.Append(map[string]interface{}{"name": "Clicky McClickHouse Jnr", "age": uint8(20)}, []interface{}{"Baby Clicky McClickHouse", uint8(1)}, map[string]string{"name": "Geoff", "id": "12123"}); err != nil {
    return err
}
if err = batch.Send(); err != nil {
    return err
}
var (
    col1 map[string]interface{}
    col2 []interface{}
    col3 map[string]string
)
// 名前付きのタプルはマップまたはスライスに取得可能、未命名はスライスのみ
if err = conn.QueryRow(ctx, "SELECT * FROM example").Scan(&col1, &col2, &col3); err != nil {
    return err
}
fmt.Printf("row: col1=%v, col2=%v, col3=%v\n", col1, col2, col3)
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/tuple.go)

Note：型付きスライスとマップは、名前付きタプルのサブカラムが全て同一の型である場合にサポートされています。

#### ネストされたフィールド

ネストされたフィールドは、名前付きのタプルの配列と同等です。使用法は、ユーザーが[flatten_nested](https://clickhouse.com/docs/ja/operations/settings/settings/#flatten-nested)を1に設定するか0に設定するかに依存します。

flatten_nestedを0に設定することで、ネストされたカラムは単一のタプルの配列として残ります。これにより、挿入と取得が簡単になります。マップのキーはカラムの名前と一致する必要があります。例：

Note: マップはタプルを表現しているため`map[string]interface{}`でなければなりません。値は現在のところ強く型付けされていません。

```go
conn, err := GetNativeConnection(clickhouse.Settings{
    "flatten_nested": 0,
}, nil, nil)
if err != nil {
    return err
}
ctx := context.Background()
defer func() {
    conn.Exec(ctx, "DROP TABLE example")
}()
conn.Exec(context.Background(), "DROP TABLE IF EXISTS example")
err = conn.Exec(ctx, `
    CREATE TABLE example (
        Col1 Nested(Col1_1 String, Col1_2 UInt8),
        Col2 Nested(
            Col2_1 UInt8,
            Col2_2 Nested(
                Col2_2_1 UInt8,
                Col2_2_2 UInt8
            )
        )
    ) Engine Memory
`)
if err != nil {
    return err
}

batch, err := conn.PrepareBatch(ctx, "INSERT INTO example")
if err != nil {
    return err
}
var i int64
for i = 0; i < 10; i++ {
    err := batch.Append(
        []map[string]interface{}{
            {
                "Col1_1": strconv.Itoa(int(i)),
                "Col1_2": uint8(i),
            },
            {
                "Col1_1": strconv.Itoa(int(i + 1)),
                "Col1_2": uint8(i + 1),
            },
            {
                "Col1_1": strconv.Itoa(int(i + 2)),
                "Col1_2": uint8(i + 2),
            },
        },
        []map[string]interface{}{
            {
                "Col2_2": []map[string]interface{}{
                    {
                        "Col2_2_1": uint8(i),
                        "Col2_2_2": uint8(i + 1),
                    },
                },
                "Col2_1": uint8(i),
            },
            {
                "Col2_2": []map[string]interface{}{
                    {
                        "Col2_2_1": uint8(i + 2),
                        "Col2_2_2": uint8(i + 3),
                    },
                },
                "Col2_1": uint8(i + 1),
            },
        },
    )
    if err != nil {
        return err
    }
}
if err := batch.Send(); err != nil {
    return err
}
var (
    col1 []map[string]interface{}
    col2 []map[string]interface{}
)
rows, err := conn.Query(ctx, "SELECT * FROM example")
if err != nil {
    return err
}
for rows.Next() {
    if err := rows.Scan(&col1, &col2); err != nil {
        return err
    }
    fmt.Printf("row: col1=%v, col2=%v\n", col1, col2)
}
rows.Close()
```

[Full Example - `flatten_tested=0`](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/nested.go#L28-L118)

`flatten_nested`のデフォルト値1を使用する場合、ネストされたカラムは別々の配列にフラット化されます。これには挿入と取得にネストされたスライスを使用する必要があります。任意のレベルのネストが動作する可能性がありますが、公式にはサポートされていません。

```go
conn, err := GetNativeConnection(nil, nil, nil)
if err != nil {
    return err
}
ctx := context.Background()
defer func() {
    conn.Exec(ctx, "DROP TABLE example")
}()
conn.Exec(ctx, "DROP TABLE IF EXISTS example")
err = conn.Exec(ctx, `
    CREATE TABLE example (
        Col1 Nested(Col1_1 String, Col1_2 UInt8),
        Col2 Nested(
            Col2_1 UInt8,
            Col2_2 Nested(
                Col2_2_1 UInt8,
                Col2_2_2 UInt8
            )
        )
    ) Engine Memory
`)
if err != nil {
    return err
}

batch, err := conn.PrepareBatch(ctx, "INSERT INTO example")
if err != nil {
    return err
}
var i uint8
for i = 0; i < 10; i++ {
    col1_1_data := []string{strconv.Itoa(int(i)), strconv.Itoa(int(i + 1)), strconv.Itoa(int(i + 2))}
    col1_2_data := []uint8{i, i + 1, i + 2}
    col2_1_data := []uint8{i, i + 1, i + 2}
    col2_2_data := [][][]interface{}{
        {
            {i, i + 1},
        },
        {
            {i + 2, i + 3},
        },
        {
            {i + 4, i + 5},
        },
    }
    err := batch.Append(
        col1_1_data,
        col1_2_data,
        col2_1_data,
        col2_2_data,
    )
    if err != nil {
        return err
    }
}
if err := batch.Send(); err != nil {
    return err
}
```

[Full Example - `flatten_nested=1`](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/nested.go#L123-L180)

Note: ネストされたカラムは同じ次元である必要があります。たとえば、上記の例では`Col_2_2`と`Col_2_1`は同一の要素数でなければなりません。

シンプルなインターフェースとネストに対する公式サポートのため、`flatten_nested=0`を推奨します。

#### 地理タイプ

クライアントは、ジオタイプのポイント、リング、ポリゴン、およびマルチポリゴンをサポートします。これらのフィールドはGolangで[github.com/paulmach/orb](https://github.com/paulmach/orb)パッケージを使用して表現します。

```go
if err = conn.Exec(ctx, `
    CREATE TABLE example (
            point Point,
            ring Ring,
            polygon Polygon,
            mPolygon MultiPolygon
        )
        Engine Memory
    `); err != nil {
    return err
}

batch, err := conn.PrepareBatch(ctx, "INSERT INTO example")
if err != nil {
    return err
}

if err = batch.Append(
    orb.Point{11, 22},
    orb.Ring{
        orb.Point{1, 2},
        orb.Point{1, 2},
    },
    orb.Polygon{
        orb.Ring{
            orb.Point{1, 2},
            orb.Point{12, 2},
        },
        orb.Ring{
            orb.Point{11, 2},
            orb.Point{1, 12},
        },
    },
    orb.MultiPolygon{
        orb.Polygon{
            orb.Ring{
                orb.Point{1, 2},
                orb.Point{12, 2},
            },
            orb.Ring{
                orb.Point{11, 2},
                orb.Point{1, 12},
            },
        },
        orb.Polygon{
            orb.Ring{
                orb.Point{1, 2},
                orb.Point{12, 2},
            },
            orb.Ring{
                orb.Point{11, 2},
                orb.Point{1, 12},
            },
        },
    },
); err != nil {
    return err
}

if err = batch.Send(); err != nil {
    return err
}

var (
    point    orb.Point
    ring     orb.Ring
    polygon  orb.Polygon
    mPolygon orb.MultiPolygon
)

if err = conn.QueryRow(ctx, "SELECT * FROM example").Scan(&point, &ring, &polygon, &mPolygon); err != nil {
    return err
}
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/geo.go)

#### UUID

UUIDタイプは[github.com/google/uuid](https://github.com/google/uuid)パッケージでサポートされています。ユーザーはまた、文字列や`sql.Scanner`や`Stringify`を実装する任意のタイプとしてUUIDを送信およびマーシャルすることができます。

```go
if err = conn.Exec(ctx, `
    CREATE TABLE example (
            col1 UUID,
            col2 UUID
        )
        Engine Memory
    `); err != nil {
    return err
}

batch, err := conn.PrepareBatch(ctx, "INSERT INTO example")
if err != nil {
    return err
}
col1Data, _ := uuid.NewUUID()
if err = batch.Append(
    col1Data,
    "603966d6-ed93-11ec-8ea0-0242ac120002",
); err != nil {
    return err
}

if err = batch.Send(); err != nil {
    return err
}

var (
    col1 uuid.UUID
    col2 uuid.UUID
)

if err = conn.QueryRow(ctx, "SELECT * FROM example").Scan(&col1, &col2); err != nil {
    return err
}
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/uuid.go)

#### 小数点タイプ

小数点タイプは[github.com/shopspring/decimal](https://github.com/shopspring/decimal)パッケージでサポートされています。

```go
if err = conn.Exec(ctx, `
    CREATE TABLE example (
        Col1 Decimal32(3),
        Col2 Decimal(18,6),
        Col3 Decimal(15,7),
        Col4 Decimal128(8),
        Col5 Decimal256(9)
    ) Engine Memory
    `); err != nil {
    return err
}

batch, err := conn.PrepareBatch(ctx, "INSERT INTO example")
if err != nil {
    return err
}
if err = batch.Append(
    decimal.New(25, 4),
    decimal.New(30, 5),
    decimal.New(35, 6),
    decimal.New(135, 7),
    decimal.New(256, 8),
); err != nil {
    return err
}

if err = batch.Send(); err != nil {
    return err
}

var (
    col1 decimal.Decimal
    col2 decimal.Decimal
    col3 decimal.Decimal
    col4 decimal.Decimal
    col5 decimal.Decimal
)

if err = conn.QueryRow(ctx, "SELECT * FROM example").Scan(&col1, &col2, &col3, &col4, &col5); err != nil {
    return err
}
fmt.Printf("col1=%v, col2=%v, col3=%v, col4=%v, col5=%v\n", col1, col2, col3, col4, col5)
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/decimal.go)

#### Nullableタイプ

GoのNil値はClickHouseのNULL値を表します。これはフィールドがNullableとして宣言されている場合に使用できます。挿入時には、通常のカラムまたはNullableバージョンのどちらにもNilを渡すことができます。前者の場合、そのタイプのデフォルト値が保存され、NullableバージョンではClickHouseにNULL値が保存されます。

Scan時には、ユーザーはNil値を表現するためにNullableフィールドのためのNilをサポートする型へのポインタを渡す必要があります。例えば、以下の例ではcol1がNullable(String)であるため、これは**stringを受け取ります。これはnilを表現するための方法です。

```go
if err = conn.Exec(ctx, `
    CREATE TABLE example (
            col1 Nullable(String),
            col2 String,
            col3 Nullable(Int8),
            col4 Nullable(Int64)
        )
        Engine Memory
    `); err != nil {
    return err
}

batch, err := conn.PrepareBatch(ctx, "INSERT INTO example")
if err != nil {
    return err
}
if err = batch.Append(
    nil,
    nil,
    nil,
    sql.NullInt64{Int64: 0, Valid: false},
); err != nil {
    return err
}

if err = batch.Send(); err != nil {
    return err
}

var (
    col1 *string
    col2 string
    col3 *int8
    col4 sql.NullInt64
)

if err = conn.QueryRow(ctx, "SELECT * FROM example").Scan(&col1, &col2, &col3, &col4); err != nil {
    return err
}
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/nullable.go)

クライアントはまた、`sql.Null*`タイプ、例えば`sql.NullInt64`もサポートしています。これらはその対になるClickHouseタイプと互換性があります。

#### 大きな整数 - Int128, Int256, UInt128, UInt256

64ビットを超える数値タイプは、ネイティブgoの[big](https://pkg.go.dev/math/big)パッケージを使用して表現されます。

```go
if err = conn.Exec(ctx, `
    CREATE TABLE example (
        Col1 Int128,
        Col2 UInt128,
        Col3 Array(Int128),
        Col4 Int256,
        Col5 Array(Int256),
        Col6 UInt256,
        Col7 Array(UInt256)
    ) Engine Memory`); err != nil {
    return err
}

batch, err := conn.PrepareBatch(ctx, "INSERT INTO example")
if err != nil {
    return err
}

col1Data, _ := new(big.Int).SetString("170141183460469231731687303715884105727", 10)
col2Data := big.NewInt(128)
col3Data := []*big.Int{
    big.NewInt(-128),
    big.NewInt(128128),
    big.NewInt(128128128),
}
col4Data := big.NewInt(256)
col5Data := []*big.Int{
    big.NewInt(256),
    big.NewInt(256256),
    big.NewInt(256256256256),
}
col6Data := big.NewInt(256)
col7Data := []*big.Int{
    big.NewInt(256),
    big.NewInt(256256),
    big.NewInt(256256256256),
}

if err = batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data, col7Data); err != nil {
    return err
}

if err = batch.Send(); err != nil {
    return err
}

var (
    col1 big.Int
    col2 big.Int
    col3 []*big.Int
    col4 big.Int
    col5 []*big.Int
    col6 big.Int
    col7 []*big.Int
)

if err = conn.QueryRow(ctx, "SELECT * FROM example").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7); err != nil {
    return err
}
fmt.Printf("col1=%v, col2=%v, col3=%v, col4=%v, col5=%v, col6=%v, col7=%v\n", col1, col2, col3, col4, col5, col6, col7)
```

[Full Example](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/big_int.go)

### 圧縮

サポートされる圧縮方法は、使用されているプロトコルに依存します。ネイティブプロトコルでは、クライアントは`LZ4`と`ZSTD`圧縮をサポートしています。その圧縮はブロックレベル上でのみ行われます。圧縮は接続時の`Compression`設定を含むことによって有効にできます。

```go
conn, err := clickhouse.Open(&clickhouse.Options{
    Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
    Auth: clickhouse.Auth{
        Database: env.Database,
        Username: env.Username,
        Password: env.Password,
    },
    Compression: &clickhouse.Compression{
        Method: clickhouse.CompressionZSTD,
    },
    MaxOpenConns: 1,
})
ctx := context.Background()
```
```go
defer func() {
    conn.Exec(ctx, "DROP TABLE example")
}()
conn.Exec(context.Background(), "DROP TABLE IF EXISTS example")
if err = conn.Exec(ctx, `
    CREATE TABLE example (
            Col1 Array(String)
    ) Engine Memory
    `); err != nil {
    return err
}
batch, err := conn.PrepareBatch(ctx, "INSERT INTO example")
if err != nil {
    return err
}
for i := 0; i < 1000; i++ {
    if err := batch.Append([]string{strconv.Itoa(i), strconv.Itoa(i + 1), strconv.Itoa(i + 2), strconv.Itoa(i + 3)}); err != nil {
        return err
    }
}
if err := batch.Send(); err != nil {
    return err
}
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/compression.go)

標準インターフェースをHTTP経由で使用する場合、追加の圧縮技術が利用可能です。詳細は[database/sql API - 圧縮](#compression)を参照してください。

### パラメータバインディング

クライアントは、Exec、クエリ、そしてQueryRowメソッドのパラメータバインディングをサポートしています。以下の例で示すように、名前付き、番号付き、位置指定パラメータを使用してサポートされています。以下にそれらの例を示します。

```go
var count uint64
// 位置バインディング
if err = conn.QueryRow(ctx, "SELECT count() FROM example WHERE Col1 >= ? AND Col3 < ?", 500, now.Add(time.Duration(750)*time.Second)).Scan(&count); err != nil {
    return err
}
// 250
fmt.Printf("位置バインディングカウント: %d\n", count)
// 数値バインディング
if err = conn.QueryRow(ctx, "SELECT count() FROM example WHERE Col1 <= $2 AND Col3 > $1", now.Add(time.Duration(150)*time.Second), 250).Scan(&count); err != nil {
    return err
}
// 100
fmt.Printf("数値バインディングカウント: %d\n", count)
// 名前付きバインディング
if err = conn.QueryRow(ctx, "SELECT count() FROM example WHERE Col1 <= @col1 AND Col3 > @col3", clickhouse.Named("col1", 100), clickhouse.Named("col3", now.Add(time.Duration(50)*time.Second))).Scan(&count); err != nil {
    return err
}
// 50
fmt.Printf("名前付きバインディングカウント: %d\n", count)
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/bind.go)

#### 特別なケース

デフォルトでは、スライスはクエリへのパラメータとして渡された場合、コンマで区切られた値のリストに展開されます。ユーザーがラップの`[ ]`で値のセットを注入する必要がある場合、ArraySetを使用するべきです。

グループ/タプルが必要である場合、IN演算子と一緒に使用すると有用な`( )`でラップされたものが、GroupSetを使用することで可能です。特に複数のグループが必要な場合に便利です。以下の例に示します。

最後に、DateTime64フィールドは、パラメータが適切にレンダリングされるよう精度が必要です。フィールドの精度レベルはクライアントからは知られないため、ユーザーが提供する必要があります。これを容易にするために、`DateNamed`パラメータを提供します。

```go
var count uint64
// 配列は展開されます
if err = conn.QueryRow(ctx, "SELECT count() FROM example WHERE Col1 IN (?)", []int{100, 200, 300, 400, 500}).Scan(&count); err != nil {
    return err
}
fmt.Printf("配列展開カウント: %d\n", count)
// 配列は[]で保持されます
if err = conn.QueryRow(ctx, "SELECT count() FROM example WHERE Col4 = ?", clickhouse.ArraySet{300, 301}).Scan(&count); err != nil {
    return err
}
fmt.Printf("配列カウント: %d\n", count)
// グループセットを使うことで( )リストを形成できます
if err = conn.QueryRow(ctx, "SELECT count() FROM example WHERE Col1 IN ?", clickhouse.GroupSet{[]interface{}{100, 200, 300, 400, 500}}).Scan(&count); err != nil {
    return err
}
fmt.Printf("グループカウント: %d\n", count)
// ネストが必要な場合により有用です
if err = conn.QueryRow(ctx, "SELECT count() FROM example WHERE (Col1, Col5) IN (?)", []clickhouse.GroupSet{{[]interface{}{100, 101}}, {[]interface{}{200, 201}}}).Scan(&count); err != nil {
    return err
}
fmt.Printf("グループカウント: %d\n", count)
// タイムに精度が必要な場合はDateNamedを使用してください#
if err = conn.QueryRow(ctx, "SELECT count() FROM example WHERE Col3 >= @col3", clickhouse.DateNamed("col3", now.Add(time.Duration(500)*time.Millisecond), clickhouse.NanoSeconds)).Scan(&count); err != nil {
    return err
}
fmt.Printf("名前付き日付のカウント: %d\n", count)
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/bind_special.go)

### コンテキストの使用

Goのコンテキストは、デッドラインやキャンセルシグナル、その他のリクエストにスコープ化された値をAPI境界間で渡す手段を提供します。コネクションの全てのメソッドは、最初の変数としてコンテキストを受け取ります。以前の例ではcontext.Background()を使用しましたが、この機能を使って設定やデッドラインを渡したり、クエリをキャンセルすることができます。

`withDeadline`を作成したコンテキストを渡すと、クエリに対して実行時間の制限を設けることができます。これは絶対的な時間であり、有効期限はコネクションを解放し、ClickHouseにキャンセル信号を送ることによってのみ解除されます。代替として`WithCancel`を使用して、クエリを明示的にキャンセルすることができます。

`clickhouse.WithQueryID`と`clickhouse.WithQuotaKey`のヘルパーにより、クエリIDと割り当てキーを指定できます。クエリIDはログでのクエリの追跡やキャンセル目的で有用です。割り当てキーは、ユニークなキー値に基づいてClickHouseの使用を制限するために使用できます - 詳細は[クオータ管理 ](https://clickhouse.com/docs/ja/operations/access-rights#quotas-management)を参照ください。

ユーザーはまた、特定のクエリにのみ設定を適用するためにコンテキストを使用して、コネクション全体には適用しません。[コネクション設定](#connection-settings)で示すように。

最後に、`clickhouse.WithBlockSize`を使用して、ブロックバッファのサイズを制御することができます。これにより、接続レベルの設定`BlockBufferSize`を上書きし、デコードされてメモリに保持されるブロックの最大数を制御します。値が大きいほど並列化が向上する可能性がありますが、メモリの消費が増える点には注意が必要です。

以下にこれらの例を示します。

```go
dialCount := 0
conn, err := clickhouse.Open(&clickhouse.Options{
    Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
    Auth: clickhouse.Auth{
        Database: env.Database,
        Username: env.Username,
        Password: env.Password,
    },
    DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
        dialCount++
        var d net.Dialer
        return d.DialContext(ctx, "tcp", addr)
    },
})
if err != nil {
    return err
}
if err := clickhouse_tests.CheckMinServerServerVersion(conn, 22, 6, 1); err != nil {
    return nil
}
// 特定のAPIコールに設定を渡すためにコンテキストを使用できます
ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
    "allow_experimental_object_type": "1",
}))

conn.Exec(ctx, "DROP TABLE IF EXISTS example")

// JSONカラムを作成するためにはallow_experimental_object_type=1が必要です
if err = conn.Exec(ctx, `
    CREATE TABLE example (
            Col1 JSON
        )
        Engine Memory
    `); err != nil {
    return err
}

// コンテキストを使用してクエリをキャンセルできます
ctx, cancel := context.WithCancel(context.Background())
go func() {
    cancel()
}()
if err = conn.QueryRow(ctx, "SELECT sleep(3)").Scan(); err == nil {
    return fmt.Errorf("キャンセルを期待していました")
}

// クエリにデッドラインを設定した場合 - 絶対時間に達した後にクエリをキャンセルします。
// ClickHouseではクエリが完了まで続行されます
ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
defer cancel()
if err := conn.Ping(ctx); err == nil {
    return fmt.Errorf("デッドライン超過を期待していました")
}

// ログでのクエリトレースを助けるためにクエリIDを設定します。例えば、system.query_logを参照してください。
var one uint8
queryId, _ := uuid.NewUUID()
ctx = clickhouse.Context(context.Background(), clickhouse.WithQueryID(queryId.String()))
if err = conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
    return err
}

conn.Exec(context.Background(), "DROP QUOTA IF EXISTS foobar")
defer func() {
    conn.Exec(context.Background(), "DROP QUOTA IF EXISTS foobar")
}()
ctx = clickhouse.Context(context.Background(), clickhouse.WithQuotaKey("abcde"))
// クォータキーを設定 - まずクォータを作成する
if err = conn.Exec(ctx, "CREATE QUOTA IF NOT EXISTS foobar KEYED BY client_key FOR INTERVAL 1 minute MAX queries = 5 TO default"); err != nil {
    return err
}

type Number struct {
    Number uint64 `ch:"number"`
}
for i := 1; i <= 6; i++ {
    var result []Number
    if err = conn.Select(ctx, &result, "SELECT number FROM numbers(10)"); err != nil {
        return err
    }
}
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/context.go)

### 進捗/プロファイル/ログ情報

クエリに関する進捗、プロファイル、およびログ情報を要求することができます。進捗情報は、ClickHouseで読み取られたおよび処理された行とバイトの統計を報告します。それに対して、プロファイル情報はクライアントに返されたデータのサマリーを提供し、バイト、行、およびブロックの合計を含みます。最後に、ログ情報はスレッドに関する統計を提供します。例えば、メモリ使用量やデータ速度などです。

この情報を取得するには、[コンテキスト](#using-context)を使用する必要があります。これにコールバック関数を渡すことができます。

```go
totalRows := uint64(0)
// コンテキストを使用して進捗およびプロファイル情報用のコールバックを渡します
ctx := clickhouse.Context(context.Background(), clickhouse.WithProgress(func(p *clickhouse.Progress) {
    fmt.Println("進捗: ", p)
    totalRows += p.Rows
}), clickhouse.WithProfileInfo(func(p *clickhouse.ProfileInfo) {
    fmt.Println("プロファイル情報: ", p)
}), clickhouse.WithLogs(func(log *clickhouse.Log) {
    fmt.Println("ログ情報: ", log)
}))

rows, err := conn.Query(ctx, "SELECT number from numbers(1000000) LIMIT 1000000")
if err != nil {
    return err
}
for rows.Next() {
}

fmt.Printf("合計行数: %d\n", totalRows)
rows.Close()
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/progress.go)

### 動的スキャン

ユーザーは、返されるフィールドのスキーマやタイプが不明なテーブルを読み取る必要があるかもしれません。これは、アドホックなデータ分析が行われたり、汎用的なツールが書かれる場合に一般的です。これを達成するために、クエリ応答にカラムタイプ情報が利用可能です。これはGoのリフレクションと組み合わせて、正しく型付けされた変数のランタイムインスタンスを作成し、Scanに渡すことができます。

```go
const query = `
SELECT
        1     AS Col1
    , 'Text' AS Col2
`
rows, err := conn.Query(context.Background(), query)
if err != nil {
    return err
}
var (
    columnTypes = rows.ColumnTypes()
    vars        = make([]interface{}, len(columnTypes))
)
for i := range columnTypes {
    vars[i] = reflect.New(columnTypes[i].ScanType()).Interface()
}
for rows.Next() {
    if err := rows.Scan(vars...); err != nil {
        return err
    }
    for _, v := range vars {
        switch v := v.(type) {
        case *string:
            fmt.Println(*v)
        case *uint8:
            fmt.Println(*v)
        }
    }
}
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/dynamic_scan_types.go)

### 外部テーブル

[外部テーブル](https://clickhouse.com/docs/ja/engines/table-engines/special/external-data/)は、クライアントがクリックハウスにデータを送信できるようにし、SELECTクエリと共にそのデータを使用することができます。このデータは一時的なテーブルに配置され、クエリ自体で評価のために使用されます。

クエリでクライアントに送信する外部データを作成するには、ext.NewTableを介して外部テーブルを構築し、これをコンテキストに渡す必要があります。

```go
table1, err := ext.NewTable("external_table_1",
    ext.Column("col1", "UInt8"),
    ext.Column("col2", "String"),
    ext.Column("col3", "DateTime"),
)
if err != nil {
    return err
}

for i := 0; i < 10; i++ {
    if err = table1.Append(uint8(i), fmt.Sprintf("value_%d", i), time.Now()); err != nil {
        return err
    }
}

table2, err := ext.NewTable("external_table_2",
    ext.Column("col1", "UInt8"),
    ext.Column("col2", "String"),
    ext.Column("col3", "DateTime"),
)

for i := 0; i < 10; i++ {
    table2.Append(uint8(i), fmt.Sprintf("value_%d", i), time.Now())
}
ctx := clickhouse.Context(context.Background(),
    clickhouse.WithExternalTable(table1, table2),
)
rows, err := conn.Query(ctx, "SELECT * FROM external_table_1")
if err != nil {
    return err
}
for rows.Next() {
    var (
        col1 uint8
        col2 string
        col3 time.Time
    )
    rows.Scan(&col1, &col2, &col3)
    fmt.Printf("col1=%d, col2=%s, col3=%v\n", col1, col2, col3)
}
rows.Close()

var count uint64
if err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM external_table_1").Scan(&count); err != nil {
    return err
}
fmt.Printf("external_table_1: %d\n", count)
if err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM external_table_2").Scan(&count); err != nil {
    return err
}
fmt.Printf("external_table_2: %d\n", count)
if err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM (SELECT * FROM external_table_1 UNION ALL SELECT * FROM external_table_2)").Scan(&count); err != nil {
    return err
}
fmt.Printf("external_table_1 UNION external_table_2: %d\n", count)
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/external_data.go)

### Open Telemetry

ClickHouseはネイティブプロトコルの一部として[トレースコンテキスト](https://clickhouse.com/docs/ja/operations/opentelemetry/)を渡すことを許可しています。クライアントは`clickhouse.withSpan`関数を介してSpanを作成し、これをコンテキスト経由で渡すことによってこれを達成します。

```go
var count uint64
rows := conn.QueryRow(clickhouse.Context(context.Background(), clickhouse.WithSpan(
    trace.NewSpanContext(trace.SpanContextConfig{
        SpanID:  trace.SpanID{1, 2, 3, 4, 5},
        TraceID: trace.TraceID{5, 4, 3, 2, 1},
    }),
)), "SELECT COUNT() FROM (SELECT number FROM system.numbers LIMIT 5)")
if err := rows.Scan(&count); err != nil {
    return err
}
fmt.Printf("count: %d\n", count)
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/clickhouse_api/open_telemetry.go)

トレースの利用についての詳細は[OpenTelemetryサポート](https://clickhouse.com/docs/ja/operations/opentelemetry/)で確認できます。

## データベース/SQL API

`database/sql`または「標準」APIは、クライアントを異なるデータベースに接続する必要がある場合に標準的なインターフェースを遵守させ、クライアントアプリケーションが基礎となるデータベースについて認識していないシナリオで使用できます。これは一定のコストを伴います - 抽象化と間接の追加レイヤーおよびそれが必ずしもClickHouseと一致しないプリミティブです。しかし、このコストは、特に複数のデータベースに接続する必要があるツーリングシナリオで受け入れ可能であることが多いです。

さらに、このクライアントはHTTPをトランスポート層として使用するHTTP構文をサポートしています - データは依然として最適なパフォーマンスのためネイティブ形式でエンコードされます。

以下はClickHouse API文書の構造をミラーリングすることを目的としています。

標準APIの完全なコード例は[こちら](https://github.com/ClickHouse/clickhouse-go/tree/main/examples/std)で確認できます。

### 接続

DSN文字列`clickhouse://<host>:<port>?<query_option>=<value>`を使用した`Open`メソッドまたは`clickhouse.OpenDB`メソッドを通じて接続を確立できます。後者は`database/sql`仕様の一部ではありませんが、`sql.DB`インスタンスが返されます。 このメソッドはプロファイリングのような機能を提供し、`database/sql`仕様を通じて公開される明白な手段がありません。

```go
func Connect() error {
	env, err := GetStdTestEnvironment()
	if err != nil {
		return err
	}
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
			Password: env.Password,
		},
	})
	return conn.Ping()
}

func ConnectDSN() error {
	env, err := GetStdTestEnvironment()
	if err != nil {
		return err
	}
	conn, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s", env.Host, env.Port, env.Username, env.Password))
	if err != nil {
		return err
	}
	return conn.Ping()
}
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/connect.go)

**以降の例では、明示的に示されない限り、ClickHouseの`conn`変数が作成され利用可能であることを仮定します。**

#### 接続設定

次のパラメータは、DSN文字列に渡すことができます。

* `hosts` - ロードバランシングとフェイルオーバーのためのシングルアドレスホストのカンマ区切りリスト - [複数のノードへの接続](#connecting-to-multiple-nodes)を参照ください。
* `username/password` - 認証情報 - [認証](#authentication)を参照ください。
* `database` - デフォルトの現在のデータベースを選択
* `dial_timeout` - 期間文字列は、オプションのフラクションと単位接尾辞を持つ、可能な限りサイン付きの一連の小数 - `300ms`、`1s`などです。 有効な時間単位は`ms`、`s`、`m`です。
* `connection_open_strategy` - `random/in_order` (デフォルト `random`) - [複数のノードへの接続](#connecting-to-multiple-nodes)を参照ください。
    - `round_robin` - セットからラウンドロビンのサーバーを選択します
    - `in_order` - 指定された順序で最初の生きているサーバーを選択します
* `debug` - デバッグ出力を有効にする (ブール値)
* `compress` - 圧縮アルゴリズムを指定 - `none`（デフォルト）、`zstd`、`lz4`、`gzip`、`deflate`、`br`。 `true`に設定されている場合、`lz4`が使われます。ネイティブ通信には`lz4`と`zstd`のみサポートされます。
* `compress_level` - 圧縮レベル（デフォルトは`0`）。圧縮詳細を参照。これはアルゴリズムに特有:
    - `gzip` - `-2`（最高速度）から`9`（最高圧縮）
    - `deflate` - `-2`（最高速度）から`9`（最高圧縮）
    - `br` - `0`（最高速度）から`11`（最高圧縮）
    - `zstd`、`lz4` - 無視されます
* `secure` - セキュアなSSL接続を確立 (デフォルトは`false`)
* `skip_verify` - 証明書検証をスキップ (デフォルトは`false`)
* `block_buffer_size` - ユーザーがブロックバッファサイズを制御できるようにします。 [BlockBufferSize](#connection-settings)を参照してください。（デフォルトは`2`）

```go
func ConnectSettings() error {
	env, err := GetStdTestEnvironment()
	if err != nil {
		return err
	}
	conn, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://127.0.0.1:9001,127.0.0.1:9002,%s:%d/%s?username=%s&password=%s&dial_timeout=10s&connection_open_strategy=round_robin&debug=true&compress=lz4", env.Host, env.Port, env.Database, env.Username, env.Password))
	if err != nil {
		return err
	}
	return conn.Ping()
}
```
[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/connect_settings.go)

#### コネクションプーリング

[複数のノードへの接続](#connecting-to-multiple-nodes)に記載されている通りに、提供されたノードアドレスのリストの使用についてユーザーは影響を与えることができます。コネクション管理とプーリングは、意図的に`sql.DB`に委ねられます。

#### HTTP経由での接続

デフォルトでは、接続はネイティブプロトコルを介して確立されます。HTTPを必要とするユーザーは、HTTPプロトコルを含むようにDSNを変更するか、接続オプションでプロトコルを指定することでこれを有効にできます。

```go
func ConnectHTTP() error {
	env, err := GetStdTestEnvironment()
	if err != nil {
		return err
	}
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.HttpPort)},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
			Password: env.Password,
		},
		Protocol: clickhouse.HTTP,
	})
	return conn.Ping()
}

func ConnectDSNHTTP() error {
	env, err := GetStdTestEnvironment()
	if err != nil {
		return err
	}
	conn, err := sql.Open("clickhouse", fmt.Sprintf("http://%s:%d?username=%s&password=%s", env.Host, env.HttpPort, env.Username, env.Password))
	if err != nil {
		return err
	}
	return conn.Ping()
}
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/connect_http.go)

#### 複数のノードへの接続

`OpenDB`を使用する場合、ClickHouse APIで使用される同じオプションアプローチを使用して複数のホストに接続します - オプションで`ConnOpenStrategy`を指定します。

DSNベースの接続の場合、文字列は複数のホストと`connection_open_strategy`パラメータを受け入れるため、この値に`round_robin`または`in_order`を設定することができます。

```go
func MultiStdHost() error {
	env, err := GetStdTestEnvironment()
	if err != nil {
		return err
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9001", "127.0.0.1:9002", fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
			Password: env.Password,
		},
		ConnOpenStrategy: clickhouse.ConnOpenRoundRobin,
	})
	if err != nil {
		return err
	}
	v, err := conn.ServerVersion()
	if err != nil {
		return err
	}
	fmt.Println(v.String())
	return nil
}

func MultiStdHostDSN() error {
	env, err := GetStdTestEnvironment()
	if err != nil {
		return err
	}
	conn, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://127.0.0.1:9001,127.0.0.1:9002,%s:%d?username=%s&password=%s&connection_open_strategy=round_robin", env.Host, env.Port, env.Username, env.Password))
	if err != nil {
		return err
	}
	return conn.Ping()
}
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/multi_host.go)

### TLSの使用

DSN接続文字列を使用する場合、パラメータ"secure=true"でSSLを有効にできます。OpenDBメソッドは、非nil TLS構造の指定に依存して[ネイティブAPIのTLS](#using-tls)と同じアプローチを使用します。DSN接続文字列はSSL検証をスキップするためのskip_verifyパラメータをサポートしていますが、OpenDBメソッドは構成を渡すことを可能にしているため、より高度なTLS構成に必要です。

```go
func ConnectSSL() error {
	env, err := GetStdTestEnvironment()
	if err != nil {
		return err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	t := &tls.Config{}
	caCert, err := ioutil.ReadFile(path.Join(cwd, "../../tests/resources/CAroot.crt"))
	if err != nil {
		return err
	}
	caCertPool := x509.NewCertPool()
	successful := caCertPool.AppendCertsFromPEM(caCert)
	if !successful {
		return err
	}
	t.RootCAs = caCertPool

	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.SslPort)},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
			Password: env.Password,
		},
		TLS: t,
	})
	return conn.Ping()
}

func ConnectDSNSSL() error {
	env, err := GetStdTestEnvironment()
	if err != nil {
		return err
	}
	conn, err := sql.Open("clickhouse", fmt.Sprintf("https://%s:%d?secure=true&skip_verify=true&username=%s&password=%s", env.Host, env.HttpsPort, env.Username, env.Password))
	if err != nil {
		return err
	}
	return conn.Ping()
}
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/ssl.go)

### 認証

OpenDBを使用する場合、通常のオプションで認証情報を渡すことができます。DSNベースの接続の場合、ユーザー名およびパスワードは接続文字列内に渡すことができ、パラメータとして、またはアドレスにエンコードされた資格情報として指定することができます。

```go
func ConnectAuth() error {
	env, err := GetStdTestEnvironment()
	if err != nil {
		return err
	}
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
			Password: env.Password,
		},
	})
	return conn.Ping()
}

func ConnectDSNAuth() error {
	env, err := GetStdTestEnvironment()
	conn, err := sql.Open("clickhouse", fmt.Sprintf("http://%s:%d?username=%s&password=%s", env.Host, env.HttpPort, env.Username, env.Password))
	if err != nil {
		return err
	}
	if err = conn.Ping(); err != nil {
		return err
	}
	conn, err = sql.Open("clickhouse", fmt.Sprintf("http://%s:%s@%s:%d", env.Username, env.Password, env.Host, env.HttpPort))
	if err != nil {
		return err
	}
	return conn.Ping()
}
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/auth.go)

### 実行

接続が確立されると、ユーザーはExecメソッドを通じて`sql`文を実行することができます。

```go
conn.Exec(`DROP TABLE IF EXISTS example`)
_, err = conn.Exec(`
    CREATE TABLE IF NOT EXISTS example (
        Col1 UInt8,
        Col2 String
    ) engine=Memory
`)
if err != nil {
    return err
}
_, err = conn.Exec("INSERT INTO example VALUES (1, 'test-1')")
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/exec.go)

このメソッドはコンテキストの受信をサポートしていません - デフォルトではバックグラウンドコンテキストで実行されます。コンテキストが必要な場合は、ExecContextを使用してください - コンテキストの使用を参照してください。

### バッチインサート

バッチのセマンティクスは`Being`メソッドを使用して`sql.Tx`を作成することによって達成できます。そこから`INSERT`文を指定して`Prepare`メソッドを使用してバッチを取得します。これにより、`sql.Stmt`が返され、`Exec`メソッドで行を追加できます。 バッチは`Commit`が最初の`sql.Tx`で実行されるまでメモリに蓄積されます。

```go
batch, err := scope.Prepare("INSERT INTO example")
if err != nil {
    return err
}
for i := 0; i < 1000; i++ {
    _, err := batch.Exec(
        uint8(42),
        "ClickHouse", "Inc",
        uuid.New(),
        map[string]uint8{"key": 1},             // Map(String, UInt8)
        []string{"Q", "W", "E", "R", "T", "Y"}, // Array(String)
        []interface{}{ // Tuple(String, UInt8, Array(Map(String, String)))
            "String Value", uint8(5), []map[string]string{
                map[string]string{"key": "value"},
                map[string]string{"key": "value"},
                map[string]string{"key": "value"},
            },
        },
        time.Now(),
    )
    if err != nil {
        return err
    }
}
return scope.Commit()
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/batch.go)

### 行のクエリ

単一の行をクエリする際は、QueryRowメソッドを使用できます。これは*sql.Rowを返し、Scanを変数へのポインタと共に呼び出してカラムをマーシャル化します。QueryRowContextバリアントは、バックグラウンド以外のコンテキストを渡すことを可能にします。

```go
row := conn.QueryRow("SELECT * FROM example")
var (
    col1             uint8
    col2, col3, col4 string
    col5             map[string]uint8
    col6             []string
    col7             interface{}
    col8             time.Time
)
if err := row.Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8); err != nil {
    return err
}
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/query_row.go)

次のメソッドを実行して複数の行を反復処理するには、`Query`メソッドを使用します。これは、行を反復処理するためにNextを呼び出せる`*sql.Rows`構造を返します。QueryContext相当は、コンテキストを渡すことを可能にします。

```go
rows, err := conn.Query("SELECT * FROM example")
if err != nil {
    return err
}
var (
    col1             uint8
    col2, col3, col4 string
    col5             map[string]uint8
    col6             []string
    col7             interface{}
    col8             time.Time
)
for rows.Next() {
    if err := rows.Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8); err != nil {
        return err
    }
    fmt.Printf("行: col1=%d, col2=%s, col3=%s, col4=%s, col5=%v, col6=%v, col7=%v, col8=%v\n", col1, col2, col3, col4, col5, col6, col7, col8)
}
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/query_rows.go)

### 非同期インサート

非同期インサートは、ExecContextメソッドを使用してインサートを実行することで達成できます。非同期モードを有効にしてコンテキストを渡す必要があります。 これにより、クライアントがサーバーがインサートを完了するのを待つか、データが受信されたら応答するかを指定できます。これにより、[wait_for_async_insert](https://clickhouse.com/docs/ja/operations/settings/settings/#wait-for-async-insert)パラメータが実質的に制御されます。

```go
const ddl = `
    CREATE TABLE example (
            Col1 UInt64
        , Col2 String
        , Col3 Array(UInt8)
        , Col4 DateTime
    ) ENGINE = Memory
    `
if _, err := conn.Exec(ddl); err != nil {
    return err
}
ctx := clickhouse.Context(context.Background(), clickhouse.WithStdAsync(false))
{
    for i := 0; i < 100; i++ {
        _, err := conn.ExecContext(ctx, fmt.Sprintf(`INSERT INTO example VALUES (
            %d, '%s', [1, 2, 3, 4, 5, 6, 7, 8, 9], now()
        )`, i, "Golang SQL database driver"))
        if err != nil {
            return err
        }
    }
}
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/async.go)

### カラム挿入

標準インターフェースを使用してのサポートはされていません。

### 構造体の使用

標準インターフェースを使用してのサポートはされていません。

### タイプ変換

標準`database/sql`インターフェースは、[ClickHouse API](#type-conversions)と同じタイプをサポートする必要があります。いくつかの例外、主に複雑なタイプについて、以下に記載します。ClickHouse APIと同様に、クライアントは挿入時の変数タイプの受け入れと応答のマーシャル化についてできるだけ柔軟であることを目指します。詳細は[タイプ変換](#type-conversions)を参照してください。

### 複雑なタイプ

特に述べられていない限り、複雑なタイプの処理は[ClickHouse API](#complex-types)と同じである必要があります。違いは`database/sql`の内部によります。

#### マップ

ClickHouse APIとは異なり、標準APIはスキャンタイプでマップの強い型付けを必要とします。例えば、`Map(String,String)`フィールドのために`map[string]interface{}`を渡すことはできず、代わりに`map[string]string`を使用する必要があります。 `interface{}`変数は常に互換性があり、より複雑な構造のために使用できます。構造体は読み取り時にサポートされていません。

```go
var (
    col1Data = map[string]uint64{
        "key_col_1_1": 1,
        "key_col_1_2": 2,
    }
    col2Data = map[string]uint64{
        "key_col_2_1": 10,
        "key_col_2_2": 20,
    }
    col3Data = map[string]uint64{}
    col4Data = []map[string]string{
        {"A": "B"},
        {"C": "D"},
    }
    col5Data = map[string]uint64{
        "key_col_5_1": 100,
        "key_col_5_2": 200,
    }
)
if _, err := batch.Exec(col1Data, col2Data, col3Data, col4Data, col5Data); err != nil {
    return err
}
if err = scope.Commit(); err != nil {
    return err
}
var (
    col1 interface{}
    col2 map[string]uint64
    col3 map[string]uint64
    col4 []map[string]string
    col5 map[string]uint64
)
if err := conn.QueryRow("SELECT * FROM example").Scan(&col1, &col2, &col3, &col4, &col5); err != nil {
    return err
}
fmt.Printf("col1=%v, col2=%v, col3=%v, col4=%v, col5=%v", col1, col2, col3, col4, col5)
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/map.go)

挿入の動作はClickHouse APIと同じです。

### 圧縮

標準APIはネイティブ[ClickHouse API](#compression)と同様の圧縮アルゴリズムをサポートしています。たとえば、ブロックレベルの`lz4`と`zstd`圧縮です。さらに、HTTP接続にはgzip、deflate、brもサポートされています。これらが有効化されている場合、圧縮はインサート時のブロックとクエリ応答のために行われます。他のリクエスト、例えばpingやクエリリクエストは圧縮されません。これは`lz4`と`zstd`オプションと一致しています。

もし`OpenDB`メソッドを使用して接続を確立する場合、圧縮の設定を渡すことができます。これには圧縮レベルを指定する能力が含まれます（以下を参照）。`sql.Open`を使用してDSNを介して接続する場合、`compress`パラメータを利用してください。これは特定の圧縮アルゴリズム、たとえば`gzip`、`deflate`、`br`、`zstd`または`lz4`あるいはブールフラグになります。`true`に設定されている場合、`lz4`が使われます。デフォルトは`none`です。すなわち、圧縮は無効です。

```go
conn := clickhouse.OpenDB(&clickhouse.Options{
    Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.HttpPort)},
    Auth: clickhouse.Auth{
        Database: env.Database,
        Username: env.Username,
        Password: env.Password,
    },
    Compression: &clickhouse.Compression{
        Method: clickhouse.CompressionBrotli,
        Level:  5,
    },
    Protocol: clickhouse.HTTP,
})
```
[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/compression.go#L27-L76)

```go
conn, err := sql.Open("clickhouse", fmt.Sprintf("http://%s:%d?username=%s&password=%s&compress=gzip&compress_level=5", env.Host, env.HttpPort, env.Username, env.Password))
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/compression.go#L78-L115)

適用された圧縮のレベルは、DSNパラメータcompress_levelまたはCompressionオプションのLevelフィールドによって制御されます。デフォルトは0ですが、これはアルゴリズムによって特有です:

* `gzip` - `-2` (ベストスピード)から`9` (ベスト圧縮)
* `deflate` - `-2` (ベストスピード)から`9` (ベスト圧縮)
* `br` - `0` (ベストスピード)から`11` (ベスト圧縮)
* `zstd`, `lz4` - 無視されます

### パラメータバインディング

標準APIは、[ClickHouse API](#parameter-binding)と同様のパラメータバインディング機能をサポートしており、Exec、Query、QueryRowメソッド（および対応する[コンテキスト](#using-context)バリアント）にパラメータを渡すことができます。位置指定、名前付き、および番号付きのパラメータがサポートされています。

```go
var count uint64
// 位置バインディング
if err = conn.QueryRow(ctx, "SELECT count() FROM example WHERE Col1 >= ? AND Col3 < ?", 500, now.Add(time.Duration(750)*time.Second)).Scan(&count); err != nil {
    return err
}
// 250
fmt.Printf("位置バインディングカウント: %d\n", count)
// 数値バインディング
if err = conn.QueryRow(ctx, "SELECT count() FROM example WHERE Col1 <= $2 AND Col3 > $1", now.Add(time.Duration(150)*time.Second), 250).Scan(&count); err != nil {
    return err
}
// 100
fmt.Printf("数値バインディングカウント: %d\n", count)
// 名前付きバインディング
if err = conn.QueryRow(ctx, "SELECT count() FROM example WHERE Col1 <= @col1 AND Col3 > @col3", clickhouse.Named("col1", 100), clickhouse.Named("col3", now.Add(time.Duration(50)*time.Second))).Scan(&count); err != nil {
    return err
}
// 50
fmt.Printf("名前付きバインディングカウント: %d\n", count)
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/bind.go)

[特別なケース](#special-cases)が適用されることに注意してください。

### コンテキストの使用

標準APIは、[ClickHouse API](#using-context)と同様に、デッドライン、キャンセルシグナル、および他のリクエストスコープの値をコンテキストを通じて渡す能力をサポートしています。ClickHouse APIとは異なり、これはメソッドの`Context`バリアントを使用することによって達成されます。たとえば、バックグラウンドコンテキストを使用する`Exec`メソッドは`ExecContext`バリアントを持ち、そこでコンテキストを最初のパラメータとして渡すことができます。これにより、アプリケーションフローの任意の段階でコンテキストを渡すことができ、たとえば、接続を確立する際に`ConnContext`またはクエリ行をリクエストする際に`QueryRowContext`を使用することができます。以下に利用可能なメソッドの例を示します。

デッドライン、キャンセルシグナル、クエリID、クォータキーおよび接続設定を渡すためにコンテキストを使用する詳細については、[ClickHouse API](#using-context)のコンテキストの使用を参照してください。

```go
ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
    "allow_experimental_object_type": "1",
}))
conn.ExecContext(ctx, "DROP TABLE IF EXISTS example")
// JSONカラムを作成するためにはallow_experimental_object_type=1が必要です
if _, err = conn.ExecContext(ctx, `
    CREATE TABLE example (
            Col1 JSON
        )
        Engine Memory
    `); err != nil {
    return err
}

// コンテキストを使用してクエリをキャンセルできます
ctx, cancel := context.WithCancel(context.Background())
go func() {
    cancel()
}()
if err = conn.QueryRowContext(ctx, "SELECT sleep(3)").Scan(); err == nil {
    return fmt.Errorf("キャンセルを期待していました")
}

// クエリにデッドラインを設定した場合 - 絶対時間に達した後にクエリをキャンセルします。再びコネクションの解除のみを行い、
// ClickHouseではクエリが完了まで続行されます
ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
defer cancel()
if err := conn.PingContext(ctx); err == nil {
    return fmt.Errorf("デッドライン超過を期待していました")
}

// ログでのクエリトレースを助けるためにクエリIDを設定します。たとえば、system.query_logを参照してください。
var one uint8
ctx = clickhouse.Context(context.Background(), clickhouse.WithQueryID(uuid.NewString()))
if err = conn.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
    return err
}

conn.ExecContext(context.Background(), "DROP QUOTA IF EXISTS foobar")
defer func() {
    conn.ExecContext(context.Background(), "DROP QUOTA IF EXISTS foobar")
}()
ctx = clickhouse.Context(context.Background(), clickhouse.WithQuotaKey("abcde"))
// クォータキーを設定 - まずクォータを作成する
if _, err = conn.ExecContext(ctx, "CREATE QUOTA IF NOT EXISTS foobar KEYED BY client_key FOR INTERVAL 1 minute MAX queries = 5 TO default"); err != nil {
    return err
}

// クエリはコンテキストを使用してキャンセルできます
ctx, cancel = context.WithCancel(context.Background())
// キャンセルの前に結果をいくつか取得します
ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
    "max_block_size": "1",
}))
rows, err := conn.QueryContext(ctx, "SELECT sleepEachRow(1), number FROM numbers(100);")
if err != nil {
    return err
}
var (
    col1 uint8
    col2 uint8
)

for rows.Next() {
    if err := rows.Scan(&col1, &col2); err != nil {
        if col2 > 3 {
            fmt.Println("キャンセルを期待していました")
            return nil
        }
        return err
    }
    fmt.Printf("行: col2=%d\n", col2)
    if col2 == 3 {
        cancel()
    }
}
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/context.go)

### セッション

ネイティブ接続は本質的にセッションを持っていますが、HTTP接続の場合、ユーザーはコンテキストを設定として渡すためにセッションIDを作成する必要があります。これにより、一時テーブルなどのセッションにバインドされた機能が使用可能になります。

```go
conn := clickhouse.OpenDB(&clickhouse.Options{
    Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.HttpPort)},
    Auth: clickhouse.Auth{
        Database: env.Database,
        Username: env.Username,
        Password: env.Password,
    },
    Protocol: clickhouse.HTTP,
    Settings: clickhouse.Settings{
        "session_id": uuid.NewString(),
    },
})
if _, err := conn.Exec(`DROP TABLE IF EXISTS example`); err != nil {
    return err
}
_, err = conn.Exec(`
    CREATE TEMPORARY TABLE IF NOT EXISTS example (
            Col1 UInt8
    )
`)
if err != nil {
    return err
}
scope, err := conn.Begin()
if err != nil {
    return err
}
batch, err := scope.Prepare("INSERT INTO example")
if err != nil {
    return err
}
for i := 0; i < 10; i++ {
    _, err := batch.Exec(
        uint8(i),
    )
    if err != nil {
        return err
    }
}
rows, err := conn.Query("SELECT * FROM example")
if err != nil {
    return err
}
var (
    col1 uint8
)
for rows.Next() {
    if err := rows.Scan(&col1); err != nil {
        return err
    }
    fmt.Printf("行: col1=%d\n", col1)
}
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/session.go)

### 動的スキャン

[ClickHouse API](#dynamic-scanning)に似て、カラムタイプ情報は利用可能で、ユーザーが実行時に正しく型付けされた変数のインスタンスを作成し、Scanに渡すことができます。これにより、タイプが未知の場合でもカラムを読み取ることができます。

```go
const query = `
SELECT
        1     AS Col1
    , 'Text' AS Col2
`
rows, err := conn.QueryContext(context.Background(), query)
if err != nil {
    return err
}
columnTypes, err := rows.ColumnTypes()
if err != nil {
    return err
}
vars := make([]interface{}, len(columnTypes))
for i := range columnTypes {
    vars[i] = reflect.New(columnTypes[i].ScanType()).Interface()
}
for rows.Next() {
    if err := rows.Scan(vars...); err != nil {
        return err
    }
    for _, v := range vars {
        switch v := v.(type) {
        case *string:
            fmt.Println(*v)
        case *uint8:
            fmt.Println(*v)
        }
    }
}
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/dynamic_scan_types.go)

### 外部テーブル

[外部テーブル](https://clickhouse.com/docs/ja/engines/table-engines/special/external-data/)は、クライアントがSELECTクエリと共にClickHouseにデータを送信できるようにします。このデータは一時的なテーブルに置かれ、クエリ自体での評価に使用可能です。

クエリでクライアントに送信する外部データを作成するためには、ext.NewTableを介して外部テーブルを構築し、これをコンテキストに渡す必要があります。

```go
table1, err := ext.NewTable("external_table_1",
    ext.Column("col1", "UInt8"),
    ext.Column("col2", "String"),
    ext.Column("col3", "DateTime"),
)
if err != nil {
    return err
}

for i := 0; i < 10; i++ {
    if err = table1.Append(uint8(i), fmt.Sprintf("value_%d", i), time.Now()); err != nil {
        return err
    }
}

table2, err := ext.NewTable("external_table_2",
    ext.Column("col1", "UInt8"),
    ext.Column("col2", "String"),
    ext.Column("col3", "DateTime"),
)

for i := 0; i < 10; i++ {
    table2.Append(uint8(i), fmt.Sprintf("value_%d", i), time.Now())
}
ctx := clickhouse.Context(context.Background(),
    clickhouse.WithExternalTable(table1, table2),
)
rows, err := conn.QueryContext(ctx, "SELECT * FROM external_table_1")
if err != nil {
    return err
}
for rows.Next() {
    var (
        col1 uint8
        col2 string
        col3 time.Time
    )
    rows.Scan(&col1, &col2, &col3)
    fmt.Printf("col1=%d, col2=%s, col3=%v\n", col1, col2, col3)
}
rows.Close()

var count uint64
if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM external_table_1").Scan(&count); err != nil {
    return err
}
fmt.Printf("external_table_1: %d\n", count)
if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM external_table_2").Scan(&count); err != nil {
    return err
}
fmt.Printf("external_table_2: %d\n", count)
if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM (SELECT * FROM external_table_1 UNION ALL SELECT * FROM external_table_2)").Scan(&count); err != nil {
    return err
}
fmt.Printf("external_table_1 UNION external_table_2: %d\n", count)
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/external_data.go)

### Open Telemetry

ClickHouseはネイティブプロトコルの一部として[トレースコンテキスト](https://clickhouse.com/docs/ja/operations/opentelemetry/)を渡すことを許可しています。クライアントは`clickhouse.withSpan`関数を介してSpanを作成し、これをコンテキスト経由で渡すことによってこれを達成します。これはHTTPをトランスポートとして使用する場合にはサポートされていません。

```go
var count uint64
rows := conn.QueryRowContext(clickhouse.Context(context.Background(), clickhouse.WithSpan(
    trace.NewSpanContext(trace.SpanContextConfig{
        SpanID:  trace.SpanID{1, 2, 3, 4, 5},
        TraceID: trace.TraceID{5, 4, 3, 2, 1},
    }),
)), "SELECT COUNT() FROM (SELECT number FROM system.numbers LIMIT 5)")
if err := rows.Scan(&count); err != nil {
    return err
}
fmt.Printf("count: %d\n", count)
```

[完全な例](https://github.com/ClickHouse/clickhouse-go/blob/main/examples/std/open_telemetry.go)

## パフォーマンステップ

* 可能であれば、ClickHouse APIを利用する。それにより、重要なリフレクションと間接を避けることができます。
* 大規模なデータセットを読み取る場合、[BlockBufferSize](#connection-settings)を変更することを検討してください。これにより、メモリのフットプリントが増えますが、行の反復中により多くのブロックが並列にデコードされるようになります。デフォルト値の2は保守的で、メモリオーバーヘッドを最小限に抑えます。高い値はメモリ内のブロック数を増やします。これはテストが必要で、異なるクエリは異なるブロックサイズを生成する可能性があります。そのため、コンテキストを介して[クエリレベル](#using-context)で設定できます。
* データを挿入する際には、できるだけ具体的に型を指定してください。クライアントはUUIDやIPのための文字列を許可するなどの柔軟さを目指していますが、これはデータの検証を必要とし、挿入時にコストを伴います。
* 可能な限り列指向のインサートを利用してください。これらは強く型付けされており、クライアントがあなたの値を変換する必要を避けます。
* ClickHouseの最適なインサートパフォーマンスのための[推奨](https://clickhouse.com/docs/ja/sql-reference/statements/insert-into/#performance-considerations)に従ってください。
```
