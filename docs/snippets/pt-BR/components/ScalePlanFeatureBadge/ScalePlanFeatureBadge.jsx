export const ScalePlanFeatureBadge = ({feature='Este recurso', linking_verb_are = false}) => {
    return (
        <div className="scalePlanFeatureContainer">
            <div className="scalePlanFeatureBadge">
                Recurso do plano Scale
            </div>
            <div>
                <p>{feature} {linking_verb_are ? 'estão' : 'está'} disponível nos planos Scale e Enterprise. Para fazer upgrade, acesse a página de planos no Console do Cloud.</p>
            </div>
        </div>
    )
}