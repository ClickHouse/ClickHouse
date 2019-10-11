workflow "Main workflow" {
  resolves = ["Label PR"]
  on = "pull_request"
}

action "Label PR" {
  uses = "decathlon/pull-request-labeler-action@v1.0.0"
  secrets = ["GITHUB_TOKEN"]
}
