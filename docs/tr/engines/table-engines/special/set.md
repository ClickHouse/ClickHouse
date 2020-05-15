---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: Koymak
---

# Koymak {#set}

Her zaman RAM olan bir veri kümesi. In operatörünün sağ tarafında kullanılmak üzere tasarlanmıştır (bölüme bakın “IN operators”).

Tabloya veri eklemek için INSERT kullanabilirsiniz. Veri kümesine yeni öğeler eklenirken, yinelenenler göz ardı edilir.
Ancak tablodan seçim yapamazsınız. Verileri almak için tek yol, IN operatörünün sağ yarısında kullanmaktır.

Veri her zaman RAM yer almaktadır. INSERT için, eklenen veri blokları da diskteki tabloların dizinine yazılır. Sunucuyu başlatırken, bu veriler RAM'e yüklenir. Başka bir deyişle, yeniden başlattıktan sonra veriler yerinde kalır.

Kaba bir sunucu yeniden başlatma için diskteki veri bloğu kaybolabilir veya zarar görebilir. İkinci durumda, dosyayı hasarlı verilerle el ile silmeniz gerekebilir.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/set/) <!--hide-->
