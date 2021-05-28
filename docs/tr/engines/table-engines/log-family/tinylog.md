---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: TinyLog
---

# TinyLog {#tinylog}

Motor log engine ailesine aittir. Görmek [Log Engine Ailesi](index.md) günlük motorlarının ortak özellikleri ve farklılıkları için.

Bu tablo motoru genellikle write-once yöntemi ile kullanılır: verileri bir kez yazın, ardından gerektiği kadar okuyun. Örneğin, kullanabilirsiniz `TinyLog`- küçük gruplar halinde işlenen Ara veriler için tablolar yazın. Çok sayıda küçük tabloda veri depolamanın verimsiz olduğunu unutmayın.

Sorgular tek bir akışta yürütülür. Başka bir deyişle, bu motor nispeten küçük tablolar için tasarlanmıştır (yaklaşık 1.000.000 satıra kadar). Çok sayıda küçük tablonuz varsa, bu tablo motorunu kullanmak mantıklıdır, çünkü [Günlük](log.md) motor (daha az dosya açılması gerekir).

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/tinylog/) <!--hide-->
