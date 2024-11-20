---
slug: /ja/native-protocol/hash
sidebar_position: 5
---

# CityHash

ClickHouseは、**古いバージョンの** [GoogleのCityHash](https://github.com/google/cityhash) を使用しています。

:::info
CityHashは、ClickHouseに追加された後にアルゴリズムが変更されました。

CityHashのドキュメントでは、特定のハッシュ値に依存せず、保存やシャーディングキーとしての使用を控えるべきであると特に注意されています。

しかし、この関数をユーザーに公開したため、CityHashのバージョン（1.0.2）を固定する必要がありました。現在、SQLで使用可能なCityHash関数の動作は変わらないことを保証しています。

— Alexey Milovidov
:::

:::note 注意

Googleの現在のCityHashのバージョンは、ClickHouseの`cityHash64`バリアントと[異なります](https://github.com/ClickHouse/ClickHouse/issues/8354)。

GoogleのCityHashの値を取得する目的で`farmHash64`を使用しないでください！[FarmHash](https://opensource.googleblog.com/2014/03/introducing-farmhash.html)はCityHashの後継ですが、完全に互換性があるわけではありません。

| 文字列                                                     | ClickHouse64         | CityHash64          | FarmHash64           |
|------------------------------------------------------------|----------------------|---------------------|----------------------|
| `Moscow`                                                   | 12507901496292878638 | 5992710078453357409 | 5992710078453357409  |
| `How can you write a big system without C++?  -Paul Glick` | 6237945311650045625  | 749291162957442504  | 11716470977470720228 |

:::

また、作成理由や説明については[Introducing CityHash](https://opensource.googleblog.com/2011/04/introducing-cityhash.html)も参照してください。要約すると、**非暗号学的な**ハッシュで、[MurmurHash](http://en.wikipedia.org/wiki/MurmurHash)よりも高速ですが、より複雑です。

## 実装

### Go

両方のバリアントを実装しているGoパッケージ[go-faster/city](https://github.com/go-faster/city)を使用できます。
