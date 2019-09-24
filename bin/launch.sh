#!/usr/bin/env bash

target=$(tmux display -p '#{session_name}:#{window_index}')
ninja -C build-dev && sleep 0.2 && (tmux send-keys -t $target.1 ./server-dev.sh C-m ; tmux split-window -d -t $target.1 -h ; tmux send-keys -t $target.2 sleep\ 1 C-m ./client-dev.sh C-m; tmux select-pane -t $target.1 -R)
