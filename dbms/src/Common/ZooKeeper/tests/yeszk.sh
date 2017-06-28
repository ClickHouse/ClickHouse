#!/usr/bin/env bash

# Выполняет действия, обратные nozk.sh

cat nozk.sh | sed 's/-A/-D/g' | bash

