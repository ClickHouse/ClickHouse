---
slug: /ja/faq/general/who-is-using-clickhouse
title: ClickHouseを使用しているのは誰ですか？
toc_hidden: true
toc_priority: 9
---

# ClickHouseを使用しているのは誰ですか？ {#who-is-using-clickhouse}

オープンソース製品であるため、この質問に答えるのはそれほど簡単ではありません。ClickHouseを使いたい場合、誰にも言う必要はなく、ソースコードや事前コンパイルパッケージを取得するだけです。契約を結ぶ必要はなく、[Apache 2.0 ライセンス](https://github.com/ClickHouse/ClickHouse/blob/master/LICENSE)によって制約のないソフトウェア配布が許可されています。

また、技術スタックはしばしばNDAでカバーされているかどうかのグレーゾーンにあります。いくつかの企業は、オープンソースであっても競争上の優位性として技術を使用すると考えており、従業員が公に詳細を共有することを許可しません。一部の企業はPRリスクがあると考えており、実装の詳細の公開はPR部門の承認が必要です。

では、ClickHouseを使用しているのは誰なのか、どうやって知るのでしょうか？

一つの方法は、**周りに聞く**ことです。書面でない場合、人々は自分の会社で使用されている技術や、使用例、使用しているハードウェアの種類、データのボリュームなどをより自由に共有します。私たちは世界中の[ClickHouse Meetup](https://www.youtube.com/channel/UChtmrD-dsdpspr42P_PyRAw/playlists)でユーザーと定期的に会話しており、1000社以上の企業がClickHouseを使用しているという話を耳にしています。残念ながら、それらは再現可能ではなく、潜在的なトラブルを避けるため、そのような話はNDAの下で語られたものと考えています。しかし、今後のミートアップに参加し、直接他のユーザーと話すことができます。ミートアップの発表方法はいくつかあり、例えば[Twitter](http://twitter.com/ClickHouseDB/)に登録できます。

二つ目の方法は、ClickHouseを使用していることを**公に発表している**企業を探すことです。これには通常、ブログ投稿、講演ビデオ、スライドデッキなどの具体的な証拠があります。私たちの**[Adopters](../../about-us/adopters.md)**ページには、そのような証拠へのリンク集を収集しており、あなたの雇用主のストーリーや見つけたリンクを自由に提供してください（プロセスでNDAを違反しないように注意してください）。

採用者リストには、Bloomberg、Cisco、中国電信、Tencent、Lyftなどの非常に大きな会社の名前が見つかりますが、最初の方法ではそれ以上の企業がいることがわかりました。例えば、[Forbesによる最大のIT企業のリスト（2020年）](https://www.forbes.com/sites/hanktucker/2020/05/13/worlds-largest-technology-companies-2020-apple-stays-on-top-zoom-and-uber-debut/)を見ても、その半分以上が何らかの形でClickHouseを使用しています。また、もともとClickHouseを2016年にオープンソース化し、ヨーロッパで最大のIT企業の一つである[Yandex](../../about-us/history.md)も忘れてはなりません。
