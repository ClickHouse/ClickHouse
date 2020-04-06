---
machine_translated: true
machine_translated_rev: f865c9653f9df092694258e0ccdd733c339112f5
toc_priority: 5
toc_title: "Caract\xE9ristiques de ClickHouse qui peuvent \xEAtre consid\xE9r\xE9\
  es comme des inconv\xE9nients"
---

# Caractéristiques de ClickHouse qui peuvent être considérées comme des inconvénients {#clickhouse-features-that-can-be-considered-disadvantages}

1.  Pas de transactions à part entière.
2.  Manque de capacité à modifier ou supprimer des données déjà insérées avec un taux élevé et une faible latence. Des suppressions et des mises à jour par lots sont disponibles pour nettoyer ou modifier les données, par exemple pour [GDPR](https://gdpr-info.eu).
3.  L'index clairsemé rend ClickHouse pas si approprié pour les requêtes ponctuelles récupérant des lignes simples par leurs clés.

[Article Original](https://clickhouse.tech/docs/en/introduction/features_considered_disadvantages/) <!--hide-->
