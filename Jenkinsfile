podTemplate(label: 'clickhouse', containers: [
  containerTemplate(name: 'docker', image: 'docker', ttyEnabled: true, command: 'cat', envVars: [
    envVar(key: 'DOCKER_HOST', value: 'tcp://docker-host-docker-host:2375')
  ])
]) {
  node('clickhouse') {
    stage('Build Image') {
      container('docker') {
        def scmVars = checkout scm

        if (env.BRANCH_NAME == "master") {
          withDockerRegistry([ credentialsId: "dockerHubCreds", url: "" ]) {
            dir("docker/build") {
              sh "docker build -t santiment/clickhouse:${env.BRANCH_NAME} ."
              sh "docker push santiment/clickhouse:${env.BRANCH_NAME}"
            }
          }
        }
      }
    }
  }
}
