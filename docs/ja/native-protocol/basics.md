---
slug: /ja/native-protocol/basics
sidebar_position: 1
---

# 基本

:::note
クライアントプロトコルのリファレンスは作成中です。

ほとんどの例はGoでのみです。
:::

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

このドキュメントは、ClickHouse TCPクライアントのバイナリプロトコルについて説明します。

## Varint

長さ、パケットコード、その他の場合には*unsigned varint*エンコーディングが使用されます。 [binary.PutUvarint](https://pkg.go.dev/encoding/binary#PutUvarint) と [binary.ReadUvarint](https://pkg.go.dev/encoding/binary#ReadUvarint) を使用します。

:::note
*Signed* varintは使用されません。
:::

## 文字列

可変長文字列は *(length, value)* としてエンコードされ、*length* は [varint](#varint) で、*value* はutf8文字列です。

:::important
OOMを防ぐために長さを検証してください:

`0 ≤ len < MAX`
:::

<Tabs>
<TabItem value="encode" label="エンコード">

```go
s := "Hello, world!"

// 文字列の長さをuvarintとして書き込む。
buf := make([]byte, binary.MaxVarintLen64)
n := binary.PutUvarint(buf, uint64(len(s)))
buf = buf[:n]

// 文字列の値を書き込む。
buf = append(buf, s...)
```

</TabItem>
<TabItem value="decode" label="デコード">

```go
r := bytes.NewReader([]byte{
    0xd, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x2c,
    0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x21,
})

// 長さを読み込む。
n, err := binary.ReadUvarint(r)
if err != nil {
	panic(err)
}

// OOMやmake()でのランタイム例外を防ぐためにnをチェックする。
const maxSize = 1024 * 1024 * 10 // 10 MB
if n > maxSize || n < 0 {
    panic("invalid n")
}

buf := make([]byte, n)
if _, err := io.ReadFull(r, buf); err != nil {
	panic(err)
}

fmt.Println(string(buf))
// Hello, world!
```

</TabItem>
</Tabs>

<Tabs>
<TabItem value="hexdump" label="ヘックスダンプ">

```hexdump
00000000  0d 48 65 6c 6c 6f 2c 20  77 6f 72 6c 64 21        |.Hello, world!|
```

</TabItem>
<TabItem value="base64" label="Base64">

```text
DUhlbGxvLCB3b3JsZCE
```

</TabItem>
<TabItem value="go" label="Go">

```go
data := []byte{
    0xd, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x2c,
    0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x21,
}
```

</TabItem>
</Tabs>

## 整数

:::tip
ClickHouseは固定サイズの整数に対して**リトルエンディアン**を使用します。
:::

### Int32
```go
v := int32(1000)

// エンコード。
buf := make([]byte, 8)
binary.LittleEndian.PutUint32(buf, uint32(v))

// デコード。
d := int32(binary.LittleEndian.Uint32(buf))
fmt.Println(d) // 1000
```

<Tabs>
<TabItem value="hexdump" label="ヘックスダンプ">

```hexdump
00000000  e8 03 00 00 00 00 00 00                           |........|
```

</TabItem>
<TabItem value="base64" label="Base64">

```text
6AMAAAAAAAA
```

</TabItem>
</Tabs>

## ブール値

ブール値は単一バイトで表され、`1` は `true`、`0` は `false` です。
