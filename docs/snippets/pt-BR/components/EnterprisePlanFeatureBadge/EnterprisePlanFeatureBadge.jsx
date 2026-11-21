export const EnterprisePlanFeatureBadge = ({feature='Este recurso', support=false, linking_verb_are = false}) => {
    return (
        <div className="enterprisePlanFeatureContainer">
            <div className="enterprisePlanFeatureBadge">
                Recurso do plano Enterprise
            </div>
            <div>
                <p>{feature} {linking_verb_are ? 'estão disponíveis' : 'está disponível'} no plano Enterprise. {support ? `Entre em contato com o suporte para habilitar este recurso.` : 'Para fazer o upgrade, acesse a página de planos no Cloud Console.'}</p>
            </div>
        </div>
    )
}