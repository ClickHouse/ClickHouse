#!/bin/bash

# Добавляет в файрвол правила, не пропускающие пакеты до серверов ZooKeeper.
# Используется для тестирования поведения программ при потере соединения с ZooKeeper.
# yeszk.sh производит обратные изменения.

# Чтобы посмотреть, какие правила сейчас есть, используйте sudo iptables -L и sudo ip6tables -L

sudo iptables -A OUTPUT -d example1 -j DROP
sudo iptables -A OUTPUT -d example2 -j DROP
sudo iptables -A OUTPUT -d example3 -j DROP
sudo ip6tables -A OUTPUT -d example1 -j DROP
sudo ip6tables -A OUTPUT -d example2 -j DROP
sudo ip6tables -A OUTPUT -d example3 -j DROP

